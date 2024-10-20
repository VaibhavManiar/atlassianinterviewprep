package vm.java.io.evm;

import java.util.List;
import java.util.TreeSet;

public class EVM {
    private final VotingRecorder votingRecorder;

    public EVM(List<Voter> voters, List<Candidate> candidates) {
        this.votingRecorder = new VotingRecorder();
    }

    private TreeSet<VotingRecorder.Record> sortedResultByCountAndWeightage() {
        return new TreeSet<>((r1, r2) -> {
            int c1 = r1.votes().size();
            int c2 = r2.votes().size();

            if (c1 == c2) {
                long w1 = r1.votes().stream().filter(vote -> Vote.Weightage.HIGH == vote.getWeightage()).count();
                long w2 = r2.votes().stream().filter(vote -> Vote.Weightage.HIGH == vote.getWeightage()).count();

                return Long.compare(w2, w1);
            }
            return Integer.compare(c2, c1);
        });
    }

    public String findWinner() {
        List<VotingRecorder.Record> result = this.votingRecorder.getVotesRecord();
        TreeSet<VotingRecorder.Record> sorterRecords = sortedResultByCountAndWeightage();
        sorterRecords.addAll(result);

        List<VotingRecorder.Record> sortedRecords = sorterRecords.stream().limit(1).toList();

        return sortedRecords.isEmpty() ? null : sortedRecords.get(0).candidateName();
    }

    public void castVote(Vote vote) {
        this.votingRecorder.record(vote);
    }
}
