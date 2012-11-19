package de.otto.jobstore.repository.impl;


import com.mongodb.*;
import de.otto.jobstore.common.*;
import de.otto.jobstore.common.properties.JobInfoProperty;
import de.otto.jobstore.repository.api.JobInfoRepository;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

public final class MongoJobInfoRepository implements JobInfoRepository {

    private static final String JOB_NAME_CLEANUP = "JobInfo_Cleanup";
    private static final String JOB_NAME_TIMED_OUT_CLEANUP = "JobInfo_TimedOut_Cleanup";

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoJobInfoRepository.class);
    private final DBCollection collection;

    public MongoJobInfoRepository(final Mongo mongo, final String dbName, final String collectionName) {
        this(mongo, dbName, collectionName, null, null);
    }

    public MongoJobInfoRepository(final Mongo mongo, final String dbName, final String collectionName, final String username, final String password) {
        final DB db = mongo.getDB(dbName);
        if (username != null && !username.isEmpty()) {
            if (db.isAuthenticated()) {
                final boolean authenticateSuccess = db.authenticate(username, password.toCharArray());
                if (!authenticateSuccess) {
                    throw new RuntimeException("The authentication at the database: " + dbName + " on the host: " +
                            mongo.getAddress() + " with the username: " + username + " and the given password was not successful");
                } else {
                    LOGGER.info("Login at database {} on the host {} was successful", dbName, mongo.getAddress());
                }
            }
        }
        collection = db.getCollection(collectionName);
        LOGGER.info("Prepare access to MongoDB collection '{}' on {}/{}", new Object[]{collectionName, mongo, dbName});
        prepareCollection();
    }

    @Override
    public String create(final String name, final long maxExecutionTime, final RunningState runningState, final boolean forceExecution) {
        return create(name, maxExecutionTime, runningState, forceExecution, new HashMap<String, String>());
    }

    @Override
    public String create(final String name, final String host, final String thread, final long maxExecutionTime,
                         final RunningState runningState, final boolean forceExecution) {
        return create(name, host, thread, maxExecutionTime, runningState, forceExecution, new HashMap<String, String>());
    }

    @Override
    public String create(final String name, final long maxExecutionTime, final RunningState runningState,
                         final boolean forceExecution, final Map<String, String> additionalData) {
        final String host = InternetUtils.getHostName();
        final String thread = Thread.currentThread().getName();
        return create(name, host, thread, maxExecutionTime, runningState, forceExecution, additionalData);
    }

    @Override
    public String create(final String name, final String host, final String thread, final long maxExecutionTime,
                         final RunningState runningState, final boolean forceExecution, final Map<String, String> additionalData) {
        try {
            LOGGER.info("Create job={} in state={} ...", name, runningState);
            final JobInfo jobInfo = new JobInfo(name, host, thread, maxExecutionTime, runningState, forceExecution, additionalData);
            save(jobInfo, WriteConcern.SAFE);
            return jobInfo.getId();
        } catch (MongoException.DuplicateKey e) {
            LOGGER.warn("job={} with state={} already exists, creation skipped!", name, runningState);
            return null;
        }
    }

    @Override
    public boolean hasRunningJob(final String name) {
        return findRunningByName(name) != null;
    }

    @Override
    public boolean hasQueuedJob(final String name) {
        return findQueuedByName(name) != null;
    }

    @Override
    public JobInfo findRunningByName(final String name) {
        final DBObject jobInfo = collection.findOne(createFindByNameAndRunningStateQuery(name, RunningState.RUNNING));
        return fromDbObject(jobInfo);
    }

    @Override
    public JobInfo findQueuedByName(final String name) {
        final DBObject jobInfo = collection.findOne(createFindByNameAndRunningStateQuery(name, RunningState.QUEUED));
        return fromDbObject(jobInfo);
    }

    @Override
    public List<JobInfo> findQueuedJobsSortedAscByCreationTime() {
        final DBCursor cursor = collection.find(new BasicDBObject(JobInfoProperty.RUNNING_STATE.val(), RunningState.QUEUED.name())).
                sort(new BasicDBObject(JobInfoProperty.CREATION_TIME.val(), SortOrder.ASC.val()));
        return getAll(cursor);
    }

    @Override
    public List<JobInfo> findByNameAndTimeRange(final String name, final Date start, final Date end) {
        final BasicDBObjectBuilder query = new BasicDBObjectBuilder().append(JobInfoProperty.NAME.val(), name);
        if (start != null) {
            query.append(JobInfoProperty.LAST_MODIFICATION_TIME.val(), new BasicDBObject(MongoOperator.GTE.op(), start));
        }
        if (end != null) {
            query.append(JobInfoProperty.LAST_MODIFICATION_TIME.val(), new BasicDBObject(MongoOperator.LTE.op(), start));
        }
        final DBCursor cursor = collection.find(query.get()).
                sort(new BasicDBObject(JobInfoProperty.CREATION_TIME.val(), SortOrder.DESC.val()));
        return getAll(cursor);
    }

    @Override
    public boolean activateQueuedJob(final String name) {
        final DBObject update = new BasicDBObject().append(MongoOperator.SET.op(),
                new BasicDBObject(JobInfoProperty.RUNNING_STATE.val(), RunningState.RUNNING.name()).
                        append(JobInfoProperty.LAST_MODIFICATION_TIME.val(), new Date()));
        LOGGER.info("Activate queued job={} ...", name);
        final WriteResult result = collection.update(createFindByNameAndRunningStateQuery(name, RunningState.QUEUED), update);
        return result.getN() == 1;
    }

    @Override
    public boolean updateHostThreadInformation(final String name) {
        return updateHostThreadInformation(name, InternetUtils.getHostName(), Thread.currentThread().getName());
    }

    @Override
    public boolean updateHostThreadInformation(final String name, final String host, final String thread) {
        final DBObject update = new BasicDBObject().append(MongoOperator.SET.op(),
                new BasicDBObject(JobInfoProperty.HOST.val(), host).append(JobInfoProperty.THREAD.val(), thread));
        final WriteResult result = collection.update(createFindByNameAndRunningStateQuery(name, RunningState.RUNNING), update);
        return result.getN() == 1;
    }

    @Override
    public boolean removeQueuedJob(final String name) {
        final WriteResult result = collection.remove(createFindByNameAndRunningStateQuery(name, RunningState.QUEUED));
        return result.getN() == 1;
    }

    @Override
    public boolean markAsFinished(final String name, final ResultState state, final String errorMessage) {
        final String uuid = UUID.randomUUID().toString();
        final Date dt = new Date();
        final BasicDBObjectBuilder set = new BasicDBObjectBuilder().
                append(JobInfoProperty.RUNNING_STATE.val(), RunningState.FINISHED + "_" + uuid).
                append(JobInfoProperty.LAST_MODIFICATION_TIME.val(), dt).
                append(JobInfoProperty.FINISH_TIME.val(), dt).
                append(JobInfoProperty.RESULT_STATE.val(), state.name());
        if (errorMessage != null) {
            set.append(JobInfoProperty.ERROR_MESSAGE.val(), errorMessage);
        }
        final DBObject update = new BasicDBObject().append(MongoOperator.SET.op(), set.get());
        final WriteResult result = collection.update(createFindByNameAndRunningStateQuery(name, RunningState.RUNNING), update);
        return result.getN() == 1;
    }

    @Override
    public boolean markAsFinished(final String name, final ResultState state) {
        return markAsFinished(name, state, null);
    }

    @Override
    public boolean markAsFinishedWithException(final String name, final Exception ex) {
        final StringWriter sw = new StringWriter();
        ex.printStackTrace(new PrintWriter(sw));
        return markAsFinished(name, ResultState.ERROR,
                "Problem: " + ex.getMessage() + ", Stack-Trace: " + sw.toString());
    }

    @Override
    public boolean markAsFinishedSuccessfully(final String name) {
        return markAsFinished(name, ResultState.SUCCESS);
    }

    @Override
    public boolean insertAdditionalData(final String name, final String key, final String value) {
        final DBObject update = new BasicDBObject().append(MongoOperator.SET.op(),
                new BasicDBObjectBuilder().append(JobInfoProperty.LAST_MODIFICATION_TIME.val(), new Date()).
                        append(JobInfoProperty.ADDITIONAL_DATA.val() + "." + key, value).get());
        final WriteResult result = collection.update(createFindByNameAndRunningStateQuery(name, RunningState.RUNNING), update);
        return result.getN() == 1;
    }

    @Override
    public JobInfo findById(final String id) {
        final DBObject obj = collection.findOne(new BasicDBObject("_id", new ObjectId(id)));
        return fromDbObject(obj);
    }

    @Override
    public List<JobInfo> findByName(final String name) {
        final BasicDBObjectBuilder query = new BasicDBObjectBuilder().append(JobInfoProperty.NAME.val(), name);
        final DBCursor cursor = collection.find(query.get()).sort(new BasicDBObject(JobInfoProperty.LAST_MODIFICATION_TIME.val(), SortOrder.DESC.val()));
        return getAll(cursor);
    }

    @Override
    public JobInfo findLastByName(final String name) {
        final DBCursor cursor = collection.find(new BasicDBObject().
                append(JobInfoProperty.NAME.val(), name)).
                sort(new BasicDBObject(JobInfoProperty.LAST_MODIFICATION_TIME.val(), SortOrder.DESC.val())).limit(1);
        return getFirst(cursor);
    }

    @Override
    public JobInfo findLastByNameAndResultState(final String name, final ResultState state) {
        final DBCursor cursor = collection.find(new BasicDBObject().
                append(JobInfoProperty.NAME.val(), name).
                append(JobInfoProperty.RESULT_STATE.val(), state.name())).
                sort(new BasicDBObject(JobInfoProperty.LAST_MODIFICATION_TIME.val(), SortOrder.DESC.val())).limit(1);
        return getFirst(cursor);
    }

    @Override
    public JobInfo findLastNotActiveByName(final String name) {
        final DBCursor cursor = collection.find(new BasicDBObject().
                append(JobInfoProperty.NAME.val(), name).
                append(JobInfoProperty.RUNNING_STATE.val(), new BasicDBObject(MongoOperator.NIN.op(),
                        Arrays.asList(RunningState.RUNNING.name(), RunningState.QUEUED.name())))).
                sort(new BasicDBObject(JobInfoProperty.LAST_MODIFICATION_TIME.val(), SortOrder.DESC.val())).limit(1);
        return getFirst(cursor);
    }

    @Override
    public List<JobInfo> findLast() {
        final List<JobInfo> jobs = new ArrayList<JobInfo>();
        for (String name : distinctJobNames()) {
            final JobInfo jobInfo = findLastByName(name);
            if (jobInfo != null) {
                jobs.add(jobInfo);
            }
        }
        return jobs;
    }

    @Override
    public List<JobInfo> findLastNotActive() {
        final List<JobInfo> jobs = new ArrayList<JobInfo>();
        for (String name : distinctJobNames()) {
            final JobInfo jobInfo = findLastNotActiveByName(name);
            if (jobInfo != null) {
                jobs.add(jobInfo);
            }
        }
        return jobs;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<String> distinctJobNames() {
        return collection.distinct(JobInfoProperty.NAME.val());
    }

    @Override
    public boolean addLoggingData(final String jobName, final String line) {
        final Date dt = new Date();
        final LogLine logLine = new LogLine(line, dt);
        final DBObject update = new BasicDBObject().
                append(MongoOperator.PUSH.op(), new BasicDBObject(JobInfoProperty.LOG_LINES.val(), logLine.toDbObject())).
                append(MongoOperator.SET.op(), new BasicDBObject(JobInfoProperty.LAST_MODIFICATION_TIME.val(), dt));
        final WriteResult result = collection.update(createFindByNameAndRunningStateQuery(jobName, RunningState.RUNNING), update);
        return result.getN() == 1;
    }

    @Override
    public void clear(final boolean dropCollection) {
        LOGGER.info("Going to clear all entities on collection: {}", collection.getFullName());
        if (dropCollection) {
            collection.drop();
            prepareCollection();
        } else {
            final WriteResult wr = collection.remove(new BasicDBObject());
            final CommandResult cr = wr.getLastError(WriteConcern.SAFE);
            if (cr.ok()) {
                LOGGER.info("Cleared all entities successfully on collection: {}", collection.getFullName());
            } else {
                LOGGER.error("Could not clear entities on collection {}: {}", collection.getFullName(), cr.getErrorMessage());
            }
        }
    }

    @Override
    public long count() {
        return collection.count();
    }

    public void cleanupTimedOutJobs() {
        final Date currentDate = new Date();
        removeJobIfTimedOut(JOB_NAME_TIMED_OUT_CLEANUP, currentDate);
        if (!hasRunningJob(JOB_NAME_TIMED_OUT_CLEANUP)) {
            create(JOB_NAME_TIMED_OUT_CLEANUP, 5 * 60 * 1000, RunningState.RUNNING, false);
            try {
                final DBCursor cursor = collection.find(new BasicDBObject(JobInfoProperty.RUNNING_STATE.val(), RunningState.RUNNING.name()));
                for (JobInfo jobInfo : getAll(cursor)) {
                    if (jobInfo.isTimedOut(currentDate)) {
                        markAsFinished(jobInfo.getName(), ResultState.TIMEOUT);
                    }
                }
                markAsFinishedSuccessfully(JOB_NAME_TIMED_OUT_CLEANUP);
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
                markAsFinishedWithException(JOB_NAME_TIMED_OUT_CLEANUP, e);
            }
        }
    }

    public void cleanupOldJobs(final int days) {
        final Date currentDate = new Date();
        removeJobIfTimedOut(JOB_NAME_CLEANUP, currentDate);
        if (!hasRunningJob(JOB_NAME_CLEANUP)) {
            create(JOB_NAME_CLEANUP, 5 * 60 * 1000, RunningState.RUNNING, false);
            cleanup(new Date(currentDate.getTime() - 1000 * 60 * 60 * 24 * Math.max(1, days)));
            markAsFinishedSuccessfully(JOB_NAME_CLEANUP);
        }
    }

    protected void save(JobInfo jobInfo) {
        collection.save(jobInfo.toDbObject());
    }

    protected void save(JobInfo jobInfo, WriteConcern writeConcern) {
        collection.save(jobInfo.toDbObject(), writeConcern);
    }

    protected void cleanup(Date clearJobsBefore) {
        collection.remove(new BasicDBObject().
                append(JobInfoProperty.LAST_MODIFICATION_TIME.val(), new BasicDBObject(MongoOperator.LT.op(), clearJobsBefore)).
                append(JobInfoProperty.RUNNING_STATE.val(), new BasicDBObject(MongoOperator.NE.op(), RunningState.RUNNING.name())));
    }

    private void removeJobIfTimedOut(final String jobName, final Date currentDate) {
        if (hasRunningJob(jobName)) {
            final JobInfo job = findRunningByName(jobName);
            if (job.isTimedOut(currentDate)) {
                markAsFinished(job.getName(), ResultState.TIMEOUT);
            }
        }
    }

    private void prepareCollection() {
        collection.ensureIndex(new BasicDBObject(JobInfoProperty.NAME.val(), 1));
        collection.ensureIndex(new BasicDBObject(JobInfoProperty.LAST_MODIFICATION_TIME.val(), 1));
        collection.ensureIndex(new BasicDBObject().
                append(JobInfoProperty.NAME.val(), 1).append(JobInfoProperty.RUNNING_STATE.val(), 1), "name_state", true);
    }

    private JobInfo fromDbObject(final DBObject dbObject) {
        if (dbObject == null) {
            return null;
        }
        return new JobInfo(dbObject);
    }

    private List<JobInfo> getAll(final DBCursor cursor) {
        final List<JobInfo> elements = new ArrayList<JobInfo>();
        while (cursor.hasNext()) {
            elements.add(fromDbObject(cursor.next()));
        }
        return elements;
    }

    private JobInfo getFirst(final DBCursor cursor) {
        if (cursor.hasNext()) {
            return fromDbObject(cursor.next());
        }
        return null;
    }

    private DBObject createFindByNameAndRunningStateQuery(final String name, final RunningState state) {
        return new BasicDBObject().append(JobInfoProperty.NAME.val(), name).
                append(JobInfoProperty.RUNNING_STATE.val(), state.name());
    }

}
