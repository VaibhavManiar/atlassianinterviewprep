import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import vm.java.io.evm.*;

import java.util.ArrayList;
import java.util.List;

public class EVMTest {

    @Test
    public void testCastVote() {
        EVM evm = new EVM(getVoters(), getCandidates());
        getDummyVotes().forEach(evm::castVote);
    }

    @Test
    public void testFindWinner() {
        EVM evm = new EVM(getVoters(), getCandidates());
        getDummyVotes().forEach(evm::castVote);
        Assertions.assertEquals("A", evm.findWinner());
    }

    @Test
    public void testFindWinnerNoVotes() {
        EVM evm = new EVM(getVoters(), getCandidates());
        getDummyVotes().forEach(evm::castVote);
        Assertions.assertNotEquals("B", evm.findWinner());
    }

    public List<Voter> getVoters() {
        List<Voter> voterList = new ArrayList<>();
        voterList.add(new Voter(1, "Abs"));
        voterList.add(new Voter(2, "xyz"));
        voterList.add(new Voter(3, "Abc"));
        voterList.add(new Voter(4, "sdf"));

        return voterList;
    }

    private List<Candidate> getCandidates() {
        List<Candidate> candidates = new ArrayList<>();
        candidates.add(new Candidate(1, "Boob"));
        candidates.add(new Candidate(2, "TIM"));
        return candidates;
    }

    private List<Vote> getDummyVotes() {
        Vote vote1 = new Vote(1, 1, "A", Vote.Weightage.HIGH);
        Vote vote2 = new Vote(1, 1, "B", Vote.Weightage.MEDIUM);
        Vote vote3 = new Vote(1, 1, "C", Vote.Weightage.LOW);

        Vote vote4 = new Vote(1, 2, "A", Vote.Weightage.HIGH);
        Vote vote5 = new Vote(1, 2, "B", Vote.Weightage.MEDIUM);
        Vote vote6 = new Vote(1, 2, "C", Vote.Weightage.LOW);

        Vote vote7 = new Vote(1, 3, "A", Vote.Weightage.LOW);
        Vote vote8 = new Vote(1, 3, "B", Vote.Weightage.HIGH);
        Vote vote9 = new Vote(1, 3, "C", Vote.Weightage.MEDIUM);

        List<Vote> voteList = new ArrayList<>();
        voteList.add(vote1);
        voteList.add(vote2);
        voteList.add(vote3);
        voteList.add(vote4);
        voteList.add(vote5);
        voteList.add(vote6);
        voteList.add(vote7);
        voteList.add(vote8);
        voteList.add(vote9);

        return voteList;
    }
}
