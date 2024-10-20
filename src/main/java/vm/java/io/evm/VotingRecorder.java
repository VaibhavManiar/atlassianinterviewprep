package vm.java.io.evm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class VotingRecorder {
    private final Map<String, List<Vote>> votesRecord = new HashMap<>();

    public void record(Vote vote) {
        this.votesRecord.computeIfAbsent(vote.getCandidateName(), k -> new ArrayList<>());
        this.votesRecord.get(vote.getCandidateName()).add(vote);
    }

    public List<Record> getVotesRecord() {
        return this.votesRecord.entrySet().stream().map(e -> new Record(e.getKey(), e.getValue())).collect(Collectors.toList());
    }

    public record Record(String candidateName, List<Vote> votes) {
    }

}
