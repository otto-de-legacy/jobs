# Job-Framework using MongoDB

## Usecase and Workmode
The framework handles the execution of jobs (local or remote) in a multi-node environment and insures that at a certain point in time only one job of a kind is executed. The node that executes a job is determined at runtime and may change each execution. It is also possible to define constraints in order to disallow execution of two different kinds of jobs at the same time.

Information collected during the execution of a job may be saved and used for monitoring. The result state of a job is captured as well. The definition of timeouts allows to detect jobs which did not finish in their expected time frame.

## Documentation
In order to use the framework you have to implement a JobRunnable interface for each job which defines their properties and execution logic. For every job executed information on it are stored in the connected MongoDB which is also used as the semaphore to only allow one job to be executed and/or queued.

### Jobservice
The Jobservice interface allows the user to control registration and execution of jobs. The executeJob methods returns the id of the executed (or queued) jobs with which the status of the job can be queried. If a job could not be executed or queued an appropriate JobException is thrown.

When starting a job an execution priority can be supplied. The effect of the execution priority is displayed in the table below.

| Priority | A job is queued | A job is running  | No job running or queued |
| :-------------: |:-------------:| :-----:| :-----:|
| lower | Job with current priority will be queued | Job with current priority will be queued | Job with current priority will be executed |
| equal | JobAlreadyQueuedException | JobAlreadyQueuedException | Job with current priority will be executed |
| higher | JobAlreadyQueuedException | JobAlreadyQueuedException | Job with current priority will be executed |

### JobRunnable
The JobRunnable interface defines the properties of a job, its execute method is executed when running the job. Before execution the prepare method is called which may contain initialization steps necessary for the job execution or check if the execution is necessary. The execute method is thus only called if the prepare method returns successfully. The afterExecution method is called after the successful execution of the execute method to allow execution of additional logic.

### JobExecutionContext
The execution context is passed to the prepage, execute and afterExecution method and may contain context information needed during the lifecycle of a job.

### JobExecutionPriority
The execution priority defines the priority with which a job is executed. It influences the behavior of the executeJob method in the JobService and also the prepare method as it should always return true for the IGNORE_PRECONDITIONS und FORCE_EXECUTION priority.

### JobInfoService
May be used to query information on jobs.

### JobInfo
Contains information about currently running and past jobs.