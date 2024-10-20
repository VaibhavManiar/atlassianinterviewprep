package vm.java.io.evm;

public class Vote {
    private final long id;
    private final long voterId;
    private final String candidateName;
    private final long timestamp;
    private final Weightage weightage;

    public Vote(long id, long voterId, String candidateName) {
        this.id = id;
        this.voterId = voterId;
        this.candidateName = candidateName;
        this.timestamp = System.currentTimeMillis();
        this.weightage = Weightage.LOW;
    }

    public Vote(long id, long voterId, String candidateName, Weightage weightage) {
        this.id = id;
        this.voterId = voterId;
        this.candidateName = candidateName;
        this.timestamp = System.currentTimeMillis();
        this.weightage = weightage;
    }

    public long getId() {
        return id;
    }

    public long getVoterId() {
        return voterId;
    }

    public String getCandidateName() {
        return candidateName;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public enum Weightage {
        HIGH(3), MEDIUM(2), LOW(1);
        private final int weight;

        Weightage(int weight) {
            this.weight = weight;
        }

        public int getWeight() {
            return weight;
        }
    }

    public Weightage getWeightage() {
        return weightage;
    }
}
