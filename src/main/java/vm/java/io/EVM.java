package vm.java.io;

import java.lang.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

public class EVM {

	private final VotingRecorder counter;
	private final Map<Long, Voter> voters;
	private final Map<Long, Candidate> candidates;

	public EVM (List<Voter> voters, List<Candidate> candidates) {
		this.voters = voters.stream().collect(Collectors.toMap(voter -> voter.getId(), voter -> voter));
		System.out.println("Voters Count : " + this.voters.size());


		this.candidates = candidates.stream().collect(Collectors.toMap(candidate -> candidate.getId(), candidate -> candidate));
		System.out.println("Candidates Count : " + this.candidates.size());

		this.counter = new VotingRecorder(this.candidates);
	}

	private PriorityQueue<Result> sortedResultByCountPQ() {
		return new PriorityQueue<Result>((r1, r2) -> {
			int c1 = r2.votes.stream().map(vote -> vote.getRank().getCount()).mapToInt(i -> i).sum();
			int c2 = r2.votes.stream().map(vote -> vote.getRank().getCount()).mapToInt(i -> i).sum();
			return Integer.compare(c2, c1);
		});
	}

	private PriorityQueue<Result> sortedResultByCountAndTimestampPQ() {
		return new PriorityQueue<>((r1, r2) -> {
            int c1 = r2.votes.stream().map(vote -> vote.getRank().getCount()).mapToInt(i -> i).sum();
            int c2 = r2.votes.stream().map(vote -> vote.getRank().getCount()).mapToInt(i -> i).sum();

            if (c1 == c2) {
                long t1 = r1.votes.get(r1.votes.size() - 1).getTimestamp();
                long t2 = r2.votes.get(r2.votes.size() - 1).getTimestamp();

                System.out.println("Vote Counts are same");
                System.out.println("R1 --> Count : " + c1 + " Timestamp : " + t1);
                System.out.println("R2 --> Count : " + c2 + " Timestamp : " + t2);

                return Long.compare(t1, t2);
            }
            return Integer.compare(c2, c1);
        });
	}

	public Candidate findWinner() {
		
		Map<Long, List<Vote>> result = this.counter.getResult();
		PriorityQueue<Result> sortedResult = sortedResultByCountAndTimestampPQ();

		System.out.println("Result Count : " + result.size());

		result.entrySet().stream()
		.map(e -> new Result(candidates.get(e.getKey()), e.getValue()))
		.map(res -> {
			System.out.println(res);
			return res;
		})
		.forEach(res -> sortedResult.add(res));

		return sortedResult.peek().candidate;
	}

	public List<Candidate> result() {
		PriorityQueue<Result> sortedResult = sortedResultByCountAndTimestampPQ();
		Map<Long, List<Vote>> result = this.counter.getResult();


		result.entrySet().stream()
			.map(e -> new Result(candidates.get(e.getKey()), e.getValue()))
			.map(res -> {
				System.out.println(res);
				return res;
			})
			.forEach(res -> sortedResult.add(res));

		return new ArrayList<>(sortedResult).stream().map(res -> res.candidate).collect(Collectors.toList());
	}

	public void castVote(Vote vote) {
		if(!voters.containsKey(vote.getVoterId()) && !candidates.containsKey(vote.getCandidateId())) {
			// TODO: Throw Exception
			return;
		}
		this.counter.increment(vote);
	}

	private static class Result {

		public Candidate candidate;
		public List<Vote> votes;

		public Result(Candidate candidate, List<Vote> votes) {
			this.candidate = candidate;
			this.votes = votes;
		}

		@Override
		public String toString() {
			return "Candidate : " + candidate + "  vote count : " + votes.size();
		}
	}

	public static class VotingRecorder {
		private final Map<Long, Candidate> candidates;
		private final Map<Long, List<Vote>> candidateVoteCount = new ConcurrentHashMap<>();

		public VotingRecorder(Map<Long, Candidate> candidates) {
			this.candidates = candidates;
		}

		public void increment(Vote vote) {
			this.candidateVoteCount.computeIfAbsent(vote.getCandidateId(), k -> new ArrayList<>());
			this.candidateVoteCount.get(vote.getCandidateId()).add(vote);
			System.out.println("Vote Casted Id : " + vote.getId());
		}

		public Map<Long, List<Vote>> getResult() {
			return this.candidateVoteCount;
		}

		public Map<Long, Candidate> getCandidates() {
			return this.candidates;
		}
	}

	public static class Candidate {
		private final long id;
		private final String name;

		public Candidate(long id, String name) {
			this.id = id;
			this.name = name;
		}

		public long getId() {
			return this.id;
		}

		public String getName() {
			return this.name;
		}

	}

	public static class Voter {
		private final long id;
		private final String name;

		public Voter(long id, String name) {
			this.id = id;
			this.name = name;
		}

		public long getId() {
			return this.id;
		}

		public String getName() {
			return this.name;
		}
	}

	public static class Vote {
		private final long id;
		private final long voterId;
		private final long candidateId;
		private final Rank rank;
		private final long timestamp;

		public Vote (long id, long voterId, long candidateId, Rank rank) {
			this.id = id;
			this.voterId = voterId;
			this.candidateId = candidateId;
			this.rank = rank;
			this.timestamp = System.currentTimeMillis();
		}

		public Vote (long id, long voterId, long candidateId) {
			this.id = id;
			this.voterId = voterId;
			this.candidateId = candidateId;
			this.rank = Rank.LOW;
			this.timestamp = System.currentTimeMillis();
		}
		
		public enum Rank {
			HIGH (3), MEDIUM(2), LOW (1);
			private final int count;
			Rank(int count) {
				this.count = count;
			}

			public int getCount() {
				return this.count;
			}
		}

		public long getId() {
			return id;
		}

		public long getVoterId() {
			return voterId;
		}

		public long getCandidateId() {
			return candidateId;
		}

		public Rank getRank() {
			return rank;
		}

		public long getTimestamp() {
			return timestamp;
		}
	}

	public static void main(String[] args) {
		List<Voter> voters = new ArrayList<>();
		voters.add(new Voter(1, "MAC"));
		voters.add(new Voter(2, "BOB"));
		voters.add(new Voter(3, "KEN"));
		voters.add(new Voter(4, "TIM"));



		List<Candidate> candidates = new ArrayList<>();
		candidates.add(new Candidate(1, "BOB"));
		candidates.add(new Candidate(2, "TIM"));


		EVM evm = new EVM(voters, candidates);

		evm.castVote(new Vote(1, 1, 1));
		evm.castVote(new Vote(3, 3, 2));
		evm.castVote(new Vote(4, 4, 2));

		try {
			Thread.sleep(10);
		} catch(InterruptedException ex) {
			System.err.println(ex.getMessage());
		}
		evm.castVote(new Vote(2, 2, 1));
		String winner = evm.findWinner().getName();
		System.out.println("Winner : " + winner);


		List<Candidate> result = evm.result();
		System.out.println("Result in decending order by count : ");
		result.forEach(candidate -> System.out.println(candidate.getName()));
		
	}
} 