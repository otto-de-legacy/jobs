package de.otto.jobstore.repository.api;

import de.otto.jobstore.common.JobInfo;
import de.otto.jobstore.common.ResultState;
import de.otto.jobstore.common.RunningState;

import java.util.Date;
import java.util.List;
import java.util.Map;

public interface JobInfoRepository {

    /***
     * Prüft ob ein Job gerade ausgeführt wird. Ein Job gilt als laufend, wenn er nicht explizit beendet wurde,
     * oder wenn das lastModifiedDate älter ist als im Timout angegeben.
     *
     * @param name Name des Jobs
     * @return true - Der Job läuft noch<br/>
     *          false - Der Job läuft nicht mehr
     */
    boolean hasRunningJob(String name);

    /***
     * Prüft ob ein Job existiert, der gequeued ist.
     *
     * @param name Name des Jobs
     * @return true - Der Job ist gequeued<br/>
     *          false - Der Job is nicht gequeued
     */
    boolean hasQueuedJob(String name);

    /***
     * Legt einen neuen Job an
     *
     * @param name Der Name des Jobs
     * @param host Der Host, auf welchem der Job ausgeführt wird
     * @param thread Der Thread, welcher den Job ausführt
     * @param maxExecutionTime Nach dieser Zeit (in ms) gilt ein Job als tot (lastModifiedTime + timeout).<br/>
     *                Ein keep alive ist durch insertOrUpdateAdditionalData möglich
     * @param state Der Status, mit dem der Job starten soll
     * @param additionalData Zusäzliche Daten, die mit dem Job gespeichert werden sollen
     * @throws IllegalAccessError Thrown if a job with the same name is already running
     */
    String create(String name, String host, String thread, long maxExecutionTime, RunningState state, Map<String, String> additionalData);

    /***
     * Legt einen neuen Job an
     *
     * @param name Der Name des Jobs
     * @param host Der Host, auf welchem der Job ausgeführt wird
     * @param thread Der Thread, welcher den Job ausführt
     * @param maxExecutionTime Nach dieser Zeit (in ms) gilt ein Job als tot (lastModifiedTime + timeout).<br/>
     *                Ein keep alive ist durch insertOrUpdateAdditionalData möglich
     * @param state Der Status, mit dem der Job starten soll
     * @throws IllegalAccessError Thrown if a job with the same name is already running
     */
    String create(String name, String host, String thread, long maxExecutionTime, RunningState state);

    /***
     * Legt einen neuen Job an
     *
     * @param name Der Name des Jobs
     * @param maxExecutionTime Nach dieser Zeit (in ms) gilt ein Job als tot (lastModifiedTime + timeout).<br/>
     *                Ein keep alive ist durch insertOrUpdateAdditionalData möglich
     * @throws IllegalAccessError Thrown if a job with the same name is already running
     */
    String create(String name, long maxExecutionTime);

    /**
     * Legt einen neuen Job an, der Name des Hosts und des Threads werden automatisch bestimmt.
     *
     * @param name Der Name des Jobs
     * @param maxExecutionTime Nach dieser Zeit (in ms) gilt ein Job als tot (lastModifiedTime + timeout).<br/>
     *                Ein keep alive ist durch insertOrUpdateAdditionalData möglich
     * @param state Der Status, mit dem der Job starten soll
     * @throws IllegalAccessError Thrown if a job with the same name is already running
     */
    String create(String name, long maxExecutionTime, RunningState state);

    /**
     * Legt einen neuen Job an, der Name des Hosts und des Threads werden automatisch bestimmt.
     *
     * @param name Der Name des Jobs
     * @param maxExecutionTime Nach dieser Zeit (in ms) gilt ein Job als tot (lastModifiedTime + timeout).<br/>
     *                Ein keep alive ist durch insertOrUpdateAdditionalData möglich
     * @param state Der Status, mit dem der Job starten soll
     * @param additionalData Zusätzliche Daten, die mit dem Job angelegt werden sollen
     * @throws IllegalAccessError Thrown if a job with the same name is already running
     */
    String create(String name, long maxExecutionTime, RunningState state, Map<String, String> additionalData);

    /**
     * Markiert einen Job als beendet und legt dessen Status fest.
     *
     * @param name Der Name des Jobs
     * @param state Der Status, mit dem der Job beendet wurde
     * @param errorMessage Eine optionale Fehlernachricht
     */
    boolean markAsFinished(String name, ResultState state, String errorMessage);

    /**
     * Markiert einen Job als beendet und legt dessen Status fest.
     *
     * @param name Der Name des Jobs
     * @param state Der Status, mit dem der Job beendet wurde
     */
    boolean markAsFinished(String name, ResultState state);

    /**
     * Markiert einen Job als beendet mit einer Exception
     *
     * @param name Der Name des Jobs
     * @param ex Die aufgetretene Exception
     */
    boolean markAsFinishedWithException(String name, Exception ex);

    /**
     * Markiert einen Job als erfolgreich beendet
     *
     * @param name Der Name des Jobs
     */
    boolean markAsFinishedSuccessfully(String name);

    /**
     * Fügt einem laufenden Job neue Informationen hinzu (Bereits existierende Informationen mit dem gleichen
     * Schlüssel werden überschrieben).
     *
     * Die Operation setzt den lastModified Timestamp auf die aktuelle Zeit
     *
     * @param name Der Name des Jobs
     * @param key Der Schlüssel unter der die Information gespeichert werden soll
     * @param value Die zu speichernde Information
     * @throws de.otto.jobstore.repository.NotFoundException Falls kein Job mit dem angegebenen Namen gefunden werden kann
     */
    boolean insertOrUpdateAdditionalData(String name, String key, String value);

    /**
     * Setzt des Status des Jobs auf active und den lastModified Timestamp auf die aktuelle Zeit
     *
     * @param name Der Name des Jobs
     * @return true - Wenn der Job aktiviert werden konnte<br/>
     *          false - Wenn der Job nicht aktiviert werden konnte (z.B. weil kein queue Job mehr existiert)
     */
    boolean activateQueuedJob(String name);

    /**
     * Lösche gequeuete Jobs mit diesem Namen
     *
     * @param name Der Name des Jobs
     * @return true fals das Löschen erfolgreich war, ansonsten false.
     */
    boolean removeQueuedJob(String name);

    boolean updateHostThreadInformation(String name, String host, String thread);

    /**
     * Liefert den Job mit dem angegebenen Namen, falls dieser läuft.
     *
     * @param name Der Name des Jobs
     * @return Liefert den laufenden Job oder null falls kein laufender Job mit dem Namen gefunden werden kann
     */
    JobInfo findRunningByName(String name);

    /**
     * Liefert den Job mit dem angegebenen Namen, falls dieser gequeueten ist.
     *
     * @param name Der Name des Jobs
     * @return Liefert den gequeueten Job oder null falls kein gequeueter Job mit dem Namen gefunden werden kann
     */
    JobInfo findQueuedByName(String name);

    List<JobInfo> findByNameAndTimeRange(String name, Date start, Date end);

    /**
     * find the jobInfo with the specified id
     *
     * @param id the id of the job
     * @return the fournd job or null, if no corresponding job was found
     */
    public JobInfo findById(String id);

    /**
     * Liefert alle Jobs mit einem bestimmten Namen.
     *
      * @param name Der Name der Jobs
     * @return Alle Jobs welche den angegebenen Namen haben
     */
    List<JobInfo> findBy(String name);

    /**
     * Liefert den Job mit dem aktuellsten last modified Datum, unabhängig von dessen Status.
     *
     * @param name Der Name des Jobs
     * @return Der Job mit dem aktuellsten last modified Datum
     */
    JobInfo findLastBy(String name);

    JobInfo findLastByResultState(String name, ResultState resultState);

    /**
     * Liefert je vorhandenem Job-Namen den Job mit dem aktuellsten last modified
     *
     * @return Liste mit dem aktuellsten Job pro Namen
     */
    List<JobInfo> findLast();

    List<JobInfo> findLastNotActive();

    JobInfo findLastNotActive(String name);

    /**
     * Liefert je vorhandenem Job-Namen den Job mit dem aktuellsten last modified, welcher nicht Idle ist
     *
     * @return Liste mit dem aktuellsten Job pro Namen, der nicht Idle ist
     */
    List<JobInfo> findLastWithNoIdleState();

    /**
     * Liefer die letzten JobInfos für alle verfügbaren Jobs
     * @return List aller Jobs inkl. JobInfos
     */
    Map<String, List<JobInfo>> distinctLastJobs(int count);

    /**
     * Liste aller unterschiedlichen Jobnamen
     *
     * @return Die unterschiedlichen Jobnamen
     */
    List<String> distinctJobNames();

    void clear();

    long count();

    /***
     * remove all inactive jobs
     */
    void cleanup();

    /**
     * Add logging information for the current running instance of this job.
     */
    boolean addLoggingData(String name, String line);

}
