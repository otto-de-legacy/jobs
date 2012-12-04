package de.otto.jobstore.service.exception;


public abstract class JobException extends Exception {

    protected JobException(String s, Throwable t) {
        super(s,t);
    }

    protected JobException(String s) {
        super(s);
    }

    //public enum Type { EXECUTION }

    //private final Type type;

    //public Type getType() {
    //    return type;
    //}

    //public JobException(Type type, String s) {
    //    super(s);
    //    this.type = type;
    //}

    //protected JobException(Type type, String s, Throwable throwable) {
    //    super(s, throwable);
    //    this.type = type;
    //}

}
