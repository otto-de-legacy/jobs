package de.otto.jobstore.repository.impl;


import com.mongodb.*;
import de.otto.jobstore.common.*;
import de.otto.jobstore.common.properties.JobInfoProperty;
import de.otto.jobstore.repository.api.IdRepository;
import de.otto.jobstore.repository.api.JobInfoRepository;
import org.bson.types.ObjectId;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

public final class MongoJobInfoRepository extends AbstractMongoRepository<JobInfo> implements JobInfoRepository {

    private static final String JOBNAME_CLEANUP          = "JobInfo_Cleanup";
    private static final String JOBNAME_TIMEDOUT_CLEANUP = "JobInfo_TimedOut_Cleanup";

    private final IdRepository idRepository;

    public MongoJobInfoRepository(Mongo mongo, String dbName, String collectionName, IdRepository idRepository) {
        super(mongo, dbName, collectionName);
        this.idRepository = idRepository;
    }

    public MongoJobInfoRepository(Mongo mongo, String dbName, String collectionName, String username, String password, IdRepository idRepository) {
        super(mongo, dbName, collectionName, username, password);
        this.idRepository = idRepository;
    }

    protected void prepareCollection() {
        collection.ensureIndex(new BasicDBObject(JobInfoProperty.NAME.val(), 1));
        collection.ensureIndex(new BasicDBObject(JobInfoProperty.LAST_MODIFICATION_TIME.val(), 1));
        collection.ensureIndex(new BasicDBObject().
                append(JobInfoProperty.NAME.val(), 1).append(JobInfoProperty.RUNNING_STATE.val(), 1), "name_state", true);
    }

    @Override
    public String create(String name, long maxExecutionTime, RunningState runningState, boolean forceExecution) {
        return create(name, maxExecutionTime, runningState, forceExecution, new HashMap<String, String>());
    }

    @Override
    public String create(String name, String host, String thread, long maxExecutionTime, RunningState runningState,
                         boolean forceExecution) {
        return create(name, host, thread, maxExecutionTime, runningState, forceExecution, new HashMap<String, String>());
    }

    @Override
    public String create(String name, long maxExecutionTime, RunningState runningState, boolean forceExecution,
                         Map<String, String> additionalData) {
        final String host = InternetUtils.getHostName();
        final String thread = Thread.currentThread().getName();
        return create(name, host, thread, maxExecutionTime, runningState, forceExecution, additionalData);
    }

    @Override
    public String create(String name, String host, String thread, long maxExecutionTime, RunningState runningState,
                         boolean forceExecution, Map<String, String> additionalData) {
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
    public boolean hasRunningJob(String name) {
        return findRunningByName(name) != null;
    }

    @Override
    public boolean hasQueuedJob(String name) {
        return findQueuedByName(name) != null;
    }

    @Override
    public JobInfo findRunningByName(String name) {
        final DBObject jobInfo = collection.findOne(createFindByNameAndRunningStateQuery(name, RunningState.RUNNING));
        return fromDbObject(jobInfo);
    }

    @Override
    public JobInfo findQueuedByName(String name) {
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
    public List<JobInfo> findByNameAndTimeRange(String name, Date start, Date end) {
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
    public boolean activateQueuedJob(String name) {
        final DBObject update = new BasicDBObject().append(MongoOperator.SET.op(),
                new BasicDBObject(JobInfoProperty.RUNNING_STATE.val(), RunningState.RUNNING.name()).
                        append(JobInfoProperty.LAST_MODIFICATION_TIME.val(), new Date()));
        boolean created;
        try {
            LOGGER.info("Activate queued job={} ...", name);
            final WriteResult result = collection.update(createFindByNameAndRunningStateQuery(name, RunningState.QUEUED), update);
            created = result.getN() == 1;
        } catch (MongoException.DuplicateKey e) {
            LOGGER.warn("Activating job with name {} failed as a running job already exists", name);
            created = false;
        }
        return created;
    }

    @Override
    public boolean updateHostThreadInformation(String name, String host, String thread) {
        final DBObject update = new BasicDBObject().append(MongoOperator.SET.op(),
                new BasicDBObject(JobInfoProperty.HOST.val(), host).append(JobInfoProperty.THREAD.val(), thread));
        final WriteResult result = collection.update(createFindByNameAndRunningStateQuery(name, RunningState.RUNNING), update);
        return result.getN() == 1;
    }

    @Override
    public boolean removeQueuedJob(String name) {
        final WriteResult result = collection.remove(createFindByNameAndRunningStateQuery(name, RunningState.QUEUED));
        return result.getN() == 1;
    }

    @Override
    public boolean markAsFinished(String name, ResultState state, String errorMessage) {
        final Long id = idRepository.getId("jobInfo");
        final Date dt = new Date();
        final BasicDBObjectBuilder set = new BasicDBObjectBuilder().
                append(JobInfoProperty.RUNNING_STATE.val(), RunningState.FINISHED + "_" + id).
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
    public boolean markAsFinished(String name, ResultState state) {
        return markAsFinished(name, state, null);
    }

    @Override
    public boolean markAsFinishedWithException(String name, Exception ex) {
        final StringWriter sw = new StringWriter();
        ex.printStackTrace(new PrintWriter(sw));
        return markAsFinished(name, ResultState.ERROR,
                "Problem: " + ex.getMessage() + ", Stack-Trace: " + sw.toString());
    }

    @Override
    public boolean markAsFinishedSuccessfully(String name) {
        return markAsFinished(name, ResultState.SUCCESS);
    }

    @Override
    public boolean insertAdditionalData(String name, String key, String value) {
        final DBObject update = new BasicDBObject().append(MongoOperator.SET.op(),
                new BasicDBObjectBuilder().append(JobInfoProperty.LAST_MODIFICATION_TIME.val(), new Date()).
                        append(JobInfoProperty.ADDITIONAL_DATA.val() + "." + key, value).get());
        final WriteResult result = collection.update(createFindByNameAndRunningStateQuery(name, RunningState.RUNNING), update);
        return result.getN() == 1;
    }

    @Override
    public JobInfo findById(String id) {
        final DBObject obj = collection.findOne(new BasicDBObject(ID, new ObjectId(id)));
        return fromDbObject(obj);
    }

    @Override
    public List<JobInfo> findByName(String name) {
        final BasicDBObjectBuilder query = new BasicDBObjectBuilder().append(JobInfoProperty.NAME.val(), name);
        final DBCursor cursor = collection.find(query.get()).sort(new BasicDBObject(JobInfoProperty.LAST_MODIFICATION_TIME.val(), SortOrder.DESC.val()));
        return getAll(cursor);
    }

    @Override
    public JobInfo findLastByName(String name) {
        final DBCursor cursor = collection.find(new BasicDBObject().
                append(JobInfoProperty.NAME.val(), name)).
                sort(new BasicDBObject(JobInfoProperty.LAST_MODIFICATION_TIME.val(), SortOrder.DESC.val())).limit(1);
        return getFirst(cursor);
    }

    @Override
    public JobInfo findLastByNameAndResultState(String name, ResultState state) {
        final DBCursor cursor = collection.find(new BasicDBObject().
                append(JobInfoProperty.NAME.val(), name).
                append(JobInfoProperty.RESULT_STATE.val(), state.name())).
                sort(new BasicDBObject(JobInfoProperty.LAST_MODIFICATION_TIME.val(), SortOrder.DESC.val())).limit(1);
        return getFirst(cursor);
    }

    @Override
    public JobInfo findLastNotActiveByName(String name) {
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
    public boolean addLoggingData(String jobName, String line) {
        final Date dt = new Date();
        final LogLine logLine = new LogLine(line, dt);
        final DBObject update = new BasicDBObject().
                append(MongoOperator.PUSH.op(), new BasicDBObject(JobInfoProperty.LOG_LINES.val(), logLine.toDbObject())).
                append(MongoOperator.SET.op(), new BasicDBObject(JobInfoProperty.LAST_MODIFICATION_TIME.val(), dt));
        final WriteResult result = collection.update(createFindByNameAndRunningStateQuery(jobName, RunningState.RUNNING), update);
        return result.getN() == 1;
    }

    public void cleanupOldJobs() {
        if (hasRunningJob(JOBNAME_CLEANUP)) {
            final JobInfo cleanupJob = findRunningByName(JOBNAME_CLEANUP);
            if (cleanupJob.isExpired(new Date())) {
                markAsFinished(cleanupJob.getName(), ResultState.TIMEOUT);
            }
        }
        if (!hasRunningJob(JOBNAME_CLEANUP)) {
            create(JOBNAME_CLEANUP, 10 * 60 * 1000, RunningState.RUNNING, false);
            try {
                cleanup(new Date(new Date().getTime() - 1000 * 60 * 60 * 24 * 5));
                markAsFinishedSuccessfully(JOBNAME_CLEANUP);
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
                markAsFinishedWithException(JOBNAME_CLEANUP, e);
            }
        }
    }

    public void cleanupTimedOutJobs() {
        final Date currentDate = new Date();
        if (hasRunningJob(JOBNAME_TIMEDOUT_CLEANUP)) {
            final JobInfo cleanupJob = findRunningByName(JOBNAME_TIMEDOUT_CLEANUP);
            if (cleanupJob.isExpired(currentDate)) {
                markAsFinished(cleanupJob.getName(), ResultState.TIMEOUT);
            }
        }
        if (!hasRunningJob(JOBNAME_TIMEDOUT_CLEANUP)) {
            create(JOBNAME_TIMEDOUT_CLEANUP, 10 * 60 * 1000, RunningState.RUNNING, false);
            try {
                final DBCursor cursor = collection.find(new BasicDBObject(JobInfoProperty.RUNNING_STATE.val(), RunningState.RUNNING.name()));
                for (JobInfo jobInfo : getAll(cursor)) {
                    if (jobInfo.isExpired(currentDate)) {
                        markAsFinished(jobInfo.getName(), ResultState.TIMEOUT);
                    }
                }
                markAsFinishedSuccessfully(JOBNAME_TIMEDOUT_CLEANUP);
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
                markAsFinishedWithException(JOBNAME_TIMEDOUT_CLEANUP, e);
            }
        }
    }

    private JobInfo fromDbObject(DBObject dbObject) {
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

    private JobInfo getFirst(DBCursor cursor) {
        if (cursor.hasNext()) {
            return fromDbObject(cursor.next());
        }
        return null;
    }

    private DBObject createFindByNameAndRunningStateQuery(String name, RunningState state) {
        return new BasicDBObject().append(JobInfoProperty.NAME.val(), name).
                append(JobInfoProperty.RUNNING_STATE.val(), state.name());
    }

    private void cleanup(Date clearJobsBefore) {
        collection.remove(new BasicDBObject().
                append(JobInfoProperty.LAST_MODIFICATION_TIME.val(), new BasicDBObject(MongoOperator.LT.op(), clearJobsBefore)).
                append(JobInfoProperty.RUNNING_STATE.val(), new BasicDBObject(MongoOperator.NE.op(), RunningState.RUNNING.name())));
    }

}
