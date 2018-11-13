package moe.cnkirito.kiritodb.data;

public interface CommitLogAware {

    CommitLog getCommitLog();

    void setCommitLog(CommitLog commitLog);
}
