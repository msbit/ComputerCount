using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using LinqToDB;
using Vec.ComputerCount.Entities;
using System.Runtime.Serialization;
using Vec.Common.Entities;
using Vec.ComputerCount.DAL;
using Vec.ComputerCount.DALReporting;

namespace Vec.ComputerCount.BL
{
    public class DistributionBL: MDistribution 
    {
        public List<BallotPaperBL> BallotPapers { get; set; }
    }

	public abstract class Calculate
	{
		protected readonly IComputerCountDataContext _computerCountDataContext;
		private readonly IReportingDAL _reporting;
		protected readonly Guid _electionElectorateVacancyId;

		protected int _Vacancies { get; set; }
		public Guid CalculationId { get; set; }
		public TieResolutionLogic TieResolutionLogic { get; set; }
		public List<BallotPaperBL> BallotPapers { get; set; }
		public TieMethodType TieMethodType { get; set; }
		public CalculationMethod CalculationMethod { get; set; }
		public int InformalPapersTotal { get; set; }
		public List<MCandidate> Candidates { get; set; }
		public List<MCandidate> InitialExcludedCandidates { get; set; } 
		public List<MCandidate> ExcludedCandidates { get; set; }
		public List<MCandidate> ElectedCandidates { get; set; }
		public List<MDistribution> Distributions { get; set; }
		public List<DistributionBL> DistributionQueue { get; set; }
		public CalculateState CalculationStatus { get; set; }

		public Calculate(IComputerCountDataContext dataContext, IReportingDAL reportingDal, Guid electionElectorateVacancyId)
		{
			_computerCountDataContext = dataContext;
			_reporting = reportingDal;
			_electionElectorateVacancyId = electionElectorateVacancyId;
		}		
		
		public virtual void LoadPapers()
		{
			var dcBallotPapers = _computerCountDataContext.LoadBallotPapersWithFrequency(_electionElectorateVacancyId);
			BallotPapers = new List<BallotPaperBL>();
			foreach (var x in dcBallotPapers)
			{
				BallotPapers.Add(new BallotPaperBL
				{
					BallotPaperId = x.BallotPaperId,
					BallotPaperNbr = x.BallotPaperNbr,
					BatchId = x.BatchId,
					CycleNo = x.CycleNo,
					Entries = x.Entries,
					ExhaustAfter = (x.ExhaustAfter == null || x.ExhaustAfter <= 0 ? x.Entries.Count : (int)x.ExhaustAfter),
					FormalFlag = x.FormalFlag,
					Frequency = x.Frequency ?? 1,
					Status = x.Status,
					VoteType = x.VoteType,
					WeightedTransferValue = x.WeightedTransferValue
				});
			}

			var candidates = BallotPapers.First().Entries.Select(x => x.Candidate).ToList();
			Candidates = candidates.OrderBy(x => x.Position).ToList();
	
			var allocatedTickets = _computerCountDataContext.GetAllocatedTickets(_electionElectorateVacancyId);
			foreach (var at in allocatedTickets)
			{
				var ticketEntries = new List<MBallotPaperEntry>();
				foreach (var p in at.Preferences)
				{
					ticketEntries.Add(new MBallotPaperEntry
					{
						BpValue = p.Value,
						Candidate = new MCandidate { Position = p.Order, Name = p.Order.ToString() },
						Mark = p.Value.ToString()
					});
				}
				BallotPapers.Add(new BallotPaperBL
				{
					CycleNo = 0,
					Entries = ticketEntries,
					ExhaustAfter = at.Preferences.Count(),
					FormalFlag = true,
					Frequency = at.Votes,
					Status = BallotPaperStatus.Entered,
					VoteType = VoteType.AllVoteTypes,
					WeightedTransferValue = 0
				});
			}

			InformalPapersTotal = _computerCountDataContext.GetInformalTotalVotes(this._electionElectorateVacancyId);
		}

		public void LoadPapers(List<BallotPaperBL> ballotpapers)
		{
			BallotPapers = ballotpapers;
		}

		public virtual List<MCandidate> ContinuingCandidates
		{
			get
			{
				return (from c in Candidates
								where !ElectedAndExcludedCandidates.Any(x => x.Position == c.Position)
								select c).ToList();
			}
		}

		public virtual int Quota
		{
			get
			{
				return ((CountPapers(BallotPapers) / (_Vacancies + 1))) + 1;
			}
		}

		public virtual List<MCandidate> ElectedAndExcludedCandidates
		{
			get
			{
				List<MCandidate> candidates = new List<MCandidate>();
				if (ElectedCandidates != null)
				{
					candidates.AddRange(ElectedCandidates);
				}
				if (ExcludedCandidates != null)
				{
					candidates.AddRange(ExcludedCandidates);
				}
				return candidates;
			}
		}
        	
		public virtual int CurrentTransfer
		{
			get
			{
				return GetMostRecentTransfer().CycleNo;
			}
		}
        
		public virtual MCandidate GetCandidate(int Position)
		{
			return Candidates.Where(c => c.Position == Position).First();
		}

        public virtual bool IsVacanciesFilled
		{
			get
			{
				return ElectedCandidates.Count >= _Vacancies;
			}
		}

		public virtual int VacanciesToFill
		{
			get
			{
				return _Vacancies - ElectedCandidates.Count;
			}
		}
		
		public virtual int CountPapers(List<BallotPaperBL> ballotpapers)
		{
			return (from b in ballotpapers
							select b.Frequency).Sum();
		}
	
		protected virtual void GetCandidateTransferValue(MCandidate candidate, List<BallotPaperBL> workingBallotPapers, ref decimal transferValue, ref int numerator, ref int denominator)
		{	
			numerator = GetCandidateProgressTotalVotes(candidate) - Quota;
			denominator = GetCandidateBallotPaperTotal(candidate, workingBallotPapers);
			transferValue = ((decimal)numerator / (decimal)denominator);
		}
		
		public virtual int GetExhaustedBallotPapersCount(List<BallotPaperBL> workingBallotPapers)
		{
			return (from bps in workingBallotPapers
							where bps.GetPreferencePosition(ElectedAndExcludedCandidates) == 0
							select bps.Frequency).Sum();
		}
	
		public virtual List<BallotPaperBL> GetCandidateBallotPapers(MCandidate candidate, List<BallotPaperBL> workingBallotPapers)
		{
			return (from w in workingBallotPapers
							where w.GetPreferencePosition(ElectedAndExcludedCandidates) == candidate.Position
							select w).ToList();
		}
		
		public virtual List<BallotPaperBL> GetCandidateBallotPapersReceivedAtTransferValue(MCandidate candidate, List<BallotPaperBL> workingBallotPapers, decimal transferValue)
		{
			return (from bl in workingBallotPapers
							where bl.GetPreferencePosition(ElectedAndExcludedCandidates) == candidate.Position
							&& bl.WeightedTransferValue == transferValue
							select bl).ToList();
		}
        
		public virtual int GetCandidateBallotPaperTotal(MCandidate candidate, List<BallotPaperBL> workingBallotPapers)
		{
			return (from bps in workingBallotPapers
							where bps.GetPreferencePosition(ElectedAndExcludedCandidates) == candidate.Position
							select bps.Frequency).Sum();
		}
		
		public virtual List<int> GetCandidatesBallotPaperTotal(List<BallotPaperBL> workingBallotPapers)
		{			
			return (from c in Candidates
							orderby c.Position
							select GetCandidateBallotPaperTotal(c, workingBallotPapers)).ToList();

		}
		
		protected virtual int GetCandidateVotes(MCandidate candidate, List<BallotPaperBL> workingBallotPapers)
		{			
			decimal sumvalues = (from bps in workingBallotPapers
													 where bps.GetPreferencePosition(ElectedAndExcludedCandidates) == candidate.Position
													 select (bps.WeightedTransferValue == 0 ? 1 : bps.WeightedTransferValue) * bps.Frequency).Sum();

			return (int)Math.Floor(sumvalues + (decimal)0.000000005);
		}
		
		protected virtual int GetCandidateVotesMultiTransferValues(MCandidate candidate, List<BallotPaperBL> workingBallotPapers)
		{		
			var groupbytransfervalue = (from bps in workingBallotPapers
																	where bps.GetPreferencePosition(ElectedAndExcludedCandidates) == candidate.Position
																	group bps by bps.WeightedTransferValue into g
																	select new { WeightedTransferValue = g.Key, SumWTV = g.Sum(w => (w.WeightedTransferValue == 0 ? 1 : w.WeightedTransferValue) * w.Frequency) });
            			
			return (int)(from grouped in groupbytransfervalue
									 select Math.Floor(grouped.SumWTV + (decimal)0.000000005)).Sum();
		}

		protected virtual List<int> GetCandidatesVotes(List<BallotPaperBL> workingBallotPapers)
		{			
			return (from c in Candidates
							orderby c.Position ascending
							select GetCandidateVotes(c, workingBallotPapers)).ToList();

		}        
	
		protected virtual MTransfer GetMostRecentTransfer()
		{
			if (Distributions.Count > 0)
			{				
				MDistribution d = (from ds in Distributions
													 orderby ds.Step descending
													 select ds).First();
				
				return (from ts in d.Transfers
								orderby ts.CycleNo descending
								select ts).First();
			}
			else
			{
				
				List<MCandidateAllocation> cas = new List<MCandidateAllocation>();

				if (Candidates.Count > 0)
				{
					foreach (MCandidate c in Candidates.OrderBy(x => x.Position))
					{
						cas.Add(new MCandidateAllocation
						{
							BallotPapers = 0,
							Candidate = c,
							CountStatus = CandidateCountStatus.Continuing,
							ElectorOrExcludeOrder = 0,
							TotalBallotPapers = 0,
							TotalVotes = 0,
							Votes = 0
						});
					}
				}

				return new MTransfer
				{
					CycleNo = 0,
					CandidateAllocations = cas,
					BallotPapersTotal = 0,
					ExhaustedPapers = 0,
					ExhaustedVotes = 0,
					TotalExhaustedVotes = 0,
					TotalExhaustedPapers = 0,
					GainLoss = 0,
					TotalGainLoss = 0,
					WeightedTransferValue = 0
				};
			}
		}
        		
		public virtual int GetCandidateProgressTotalVotes(MCandidate candidate)
		{			
			List<MCandidateAllocation> lastallocations = GetMostRecentTransfer().CandidateAllocations;
			return (from ca in lastallocations
							where ca.Candidate.Position == candidate.Position
							select ca.TotalVotes).FirstOrDefault();
		}
		
		protected virtual string GetFromCycleNumbers(MCandidate candidate, decimal weightedTransferValue)
		{
			string returnval = string.Empty;
			List<int> fromCycleList = new List<int>();

			foreach (MDistribution d in (from ds in Distributions where ds.DistributionType != DistributionType.Initial select ds))
			{
				foreach (MTransfer t in d.Transfers.Where(x => x.WeightedTransferValue == weightedTransferValue))
				{
					if (t.CandidateAllocations.Where(x => x.Candidate.Position == candidate.Position).First().BallotPapers > 0)
					{
						fromCycleList.Add(t.CycleNo);
					}
				}
			}

			fromCycleList.Sort();
			returnval = String.Join(",", fromCycleList);
			return returnval;
		}
		
		protected virtual int GetSumVotesReceivedByCandidateAtTransferValue(MCandidate candidate, decimal weightedTransferValue)
		{
			int total = 0;
			foreach (MDistribution d in Distributions)
			{
				List<MTransfer> ts = (from t in d.Transfers
															where t.WeightedTransferValue == weightedTransferValue
															select t).ToList();

				foreach (MTransfer t in ts)
				{
					MCandidateAllocation allocation = (from ca in t.CandidateAllocations
																						 where ca.Candidate.Position == candidate.Position
																						 select ca).First();
					total += allocation.Votes;
				}
			}
			return total;
		}
		
		public virtual List<MCandidate> GetPassedQuotaCandidates()
		{			
			List<MCandidateAllocation> lastallocations = GetMostRecentTransfer().CandidateAllocations;
			return (from ca in lastallocations
							where !ElectedAndExcludedCandidates.Any(x => x.Position == ca.Candidate.Position)
							&& ca.TotalVotes >= Quota
							orderby ca.TotalVotes descending, ca.Candidate.Position
							select ca.Candidate).ToList();
		}
		
		public virtual List<MCandidate> GetCandidatesWithHighestVotes()
		{			
			List<MCandidateAllocation> lastallocations = GetMostRecentTransfer().CandidateAllocations;
			int maxvotes = (from ca in lastallocations
											where !ElectedAndExcludedCandidates.Any(x => x.Position == ca.Candidate.Position)
											orderby ca.TotalVotes descending
											select ca.TotalVotes).FirstOrDefault();

			return (from ca in lastallocations
							where ca.TotalVotes == maxvotes
							&& !ElectedAndExcludedCandidates.Any(x => x.Position == ca.Candidate.Position)
							orderby ca.Candidate.Position
							select ca.Candidate).ToList();
		}

		public virtual MCandidate GetCandidatesWithLowestVotesFrom(List<MCandidate> excluding)
		{			
			List<MCandidateAllocation> lastallocations = GetMostRecentTransfer().CandidateAllocations;

			return (from ca in lastallocations
							where excluding.Any(x => x.Position == ca.Candidate.Position)
							orderby ca.TotalVotes ascending
							select ca.Candidate).First();
		}
		
		public virtual List<MCandidate> GetCandidatesWithLowestVotes()
		{		
			List<MCandidateAllocation> lastallocations = GetMostRecentTransfer().CandidateAllocations;
			int minvotes = (from ca in lastallocations
											where !ElectedAndExcludedCandidates.Any(x => x.Position == ca.Candidate.Position)
											orderby ca.TotalVotes ascending
											select ca.TotalVotes).First();
			return (from ca in lastallocations
							where ca.TotalVotes == minvotes
							&& !ElectedAndExcludedCandidates.Any(x => x.Position == ca.Candidate.Position)
							orderby ca.Candidate.Position
							select ca.Candidate).ToList();
		}

		public virtual List<MCandidateEx> GetSurplusTiedCandidates(List<MCandidate> candidates)
		{
			List<MCandidateAllocation> lastallocations = GetMostRecentTransfer().CandidateAllocations;
		
			var result = (from ca in lastallocations
										where !ElectedAndExcludedCandidates.Any(x => x.Position == ca.Candidate.Position) &&
										candidates.Any(x => x.Position == ca.Candidate.Position) &&
										ca.TotalVotes >= Quota
										let mex = new MCandidateEx(ca.Candidate) { CurrentTotalVotes = ca.TotalVotes }
										orderby ca.TotalVotes descending, ca.Candidate.Position
										select mex).ToList();

			foreach (MCandidateEx mex in result)
			{
				mex.IsTie = result.Any(x => x.CurrentTotalVotes == mex.CurrentTotalVotes && x.Position != mex.Position);
			}

			return result;
		}

		public virtual void SetCandidateCountStatus(MCandidate candidate, CandidateCountStatus ccs)
		{
			MTransfer lasttransfer = GetMostRecentTransfer();
			int maxorder = 0;
			if (lasttransfer.CandidateAllocations.Count > 0)
			{
				maxorder = lasttransfer.CandidateAllocations.Select(x => x.ElectorOrExcludeOrder).Max();
			}
			MCandidateAllocation cas = (from ca in lasttransfer.CandidateAllocations where ca.Candidate.Position == candidate.Position select ca).FirstOrDefault();
			if (cas != null)
			{
				cas.CountStatus = ccs;
				if (ccs == CandidateCountStatus.Elected || ccs == CandidateCountStatus.Excluded)
				{
					cas.ElectorOrExcludeOrder = maxorder + 1;
				}
			}
		}

		protected virtual List<MCandidate> GetHighestDifferenceInCycle(List<MCandidate> tiedCandidatesToResolve)
		{
			List<MCandidate> resolvingCandidates = new List<MCandidate>(tiedCandidatesToResolve);

			foreach (MDistribution d in Distributions.OrderByDescending(x => x.Step))
			{
				if (resolvingCandidates.Count > 1)
				{
					foreach (MTransfer t in d.Transfers.OrderByDescending(x => x.CycleNo))
					{
						var ca = (from cas in t.CandidateAllocations
											join tied in resolvingCandidates on cas.Candidate.Position equals tied.Position
											orderby cas.TotalVotes descending
											select new { tied.Position, cas.TotalVotes });

						List<int> distinctTotalVotes = (from c in ca
																						select c.TotalVotes).Distinct().ToList();

						if (distinctTotalVotes.Count() > 1)
						{
							int highesttotal = distinctTotalVotes.OrderByDescending(x => x).First();
							
							var highestTotalCandidates = (from c in ca
																						where c.TotalVotes == highesttotal
																						select c).ToList();

							List<MCandidate> candidatesToRemove = (from rc in resolvingCandidates
																										 where !highestTotalCandidates.Exists(x => x.Position == rc.Position)
																										 select rc).ToList();

							foreach (MCandidate c in candidatesToRemove)
							{
								resolvingCandidates.Remove(c);
							}

						}
					}
				}
			}

			return resolvingCandidates;

		}
		
		public virtual void Distribute(MCandidate candidate, List<BallotPaperBL> workingpapers, DistributionType distType, decimal transferValue, int transferVotes)
		{
			int maxdistribution = 0;
			MDistribution lastDistribution = new MDistribution();
			MDistribution thisDistribution = new MDistribution();
			bool newDistribution = false;
			
			if (Distributions.Count > 0)
			{
				maxdistribution = Distributions.Select(x => x.Step).Max();
				lastDistribution = (from ds in Distributions
														where ds.Step == maxdistribution
														select ds).FirstOrDefault();
			}
            			
			if (lastDistribution.Candidate == null ||
					lastDistribution.Candidate.Position != candidate.Position)
			{				
				thisDistribution = new MDistribution()
				{
					DistributionType = distType,
					Step = maxdistribution + 1,
					Candidate = candidate,
					Transfers = new List<MTransfer>()
				};
				newDistribution = true;
			}
			else
			{
				thisDistribution = lastDistribution;
			}

			thisDistribution.Transfers.Add(CreateNewTransfer(distType, candidate, workingpapers, transferValue, transferVotes));

			if (newDistribution)
			{
				Distributions.Add(thisDistribution);
			}
		}

		
		protected virtual MTransfer CreateNewTransfer(DistributionType distType, MCandidate candidate, List<BallotPaperBL> workingpapers, decimal transferValue, int transferVotes)
		{
			string fromCycles = string.Empty;
			int workingpaperscount = CountPapers(workingpapers);

			MTransfer lasttransfer = GetMostRecentTransfer();
			List<MCandidateAllocation> lastallocations = (from cas in lasttransfer.CandidateAllocations
																										orderby cas.Candidate.Position
																										select cas).ToList();
			
			if (distType == DistributionType.Surplus && transferValue > 0 && transferValue < 1)
			{
				workingpapers.ForEach(x => x.WeightedTransferValue = transferValue);
			}

			List<int> addedpapers = GetCandidatesBallotPaperTotal(workingpapers);
			List<int> addedvotes = GetCandidatesVotes(workingpapers);
			int totaladdedvotes = addedvotes.Sum();

			if (distType == DistributionType.Exclusion && workingpaperscount > 0 && candidate != null)
			{
				if (transferValue == 0)
				{
					fromCycles = "1";
				}
				else
				{
					fromCycles = GetFromCycleNumbers(candidate, transferValue);
				}
			}

			if (distType == DistributionType.Exclusion && transferValue == 0)
			{
				workingpapers.ForEach(x => x.WeightedTransferValue = 1);
				transferValue = 1;
			}

			int exhaustedpapers = GetExhaustedBallotPapersCount(workingpapers);
			int exhaustedvalue = (int)Math.Floor((decimal)exhaustedpapers * transferValue);
			int gainloss = transferVotes - totaladdedvotes - exhaustedvalue;
			
			MTransfer t = new MTransfer()
			{
				BallotPapersTotal = workingpaperscount,
				VotesTotal = transferVotes,
				CycleNo = lasttransfer.CycleNo + 1,
				FromCycles = fromCycles,
				ExhaustedPapers = exhaustedpapers,
				ExhaustedVotes = exhaustedvalue,
				GainLoss = gainloss,
				TotalExhaustedPapers = lasttransfer.TotalExhaustedPapers + exhaustedpapers,
				TotalExhaustedVotes = lasttransfer.TotalExhaustedVotes + exhaustedvalue,
				TotalGainLoss = lasttransfer.TotalGainLoss + gainloss,
				WeightedTransferValue = transferValue,
				CandidateAllocations = new List<MCandidateAllocation>()
			};

			int position = 0;
			foreach (MCandidateAllocation lastca in lastallocations)
			{
				if (distType != DistributionType.Initial)
				{
					if ((position + 1) == candidate.Position)
					{
						addedpapers[position] = (workingpaperscount * -1);
						addedvotes[position] = (transferVotes * -1);
					}
				}

				MCandidateAllocation newca = new MCandidateAllocation()
				{
					BallotPapers = addedpapers[position],
					Votes = addedvotes[position],
					Candidate = GetCandidate(position + 1),
					CountStatus = lastca.CountStatus,
					ElectorOrExcludeOrder = lastca.ElectorOrExcludeOrder,
					TotalBallotPapers = lastca.TotalBallotPapers + addedpapers[position],
					TotalVotes = lastca.TotalVotes + addedvotes[position]
				};
				t.CandidateAllocations.Add(newca);
				position += 1;
			}
			
			workingpapers.ForEach(x => x.CycleNo = lasttransfer.CycleNo + 1);

			return t;
		}
        		
		protected int DetermineTotalVotesForTransfer(MCandidate candidate, DistributionType distType, int ballotPaperCount, decimal transferValue)
		{
			int transferVotes = 0;
			var lasttransfer = GetMostRecentTransfer();

			if (transferValue != 1 && transferValue != 0)
			{
				switch (distType)
				{
					case DistributionType.Exclusion:
						transferVotes = GetSumVotesReceivedByCandidateAtTransferValue(candidate, transferValue);
						break;
					case DistributionType.Surplus:
						int candidatePTotal = (from ca in lasttransfer.CandidateAllocations
																	 where ca.Candidate.Position == candidate.Position
																	 select ca.TotalVotes).First();
						transferVotes = candidatePTotal - Quota;
						break;
					default:
						transferVotes = ballotPaperCount;
						break;
				}
			}
			else
			{				
				transferVotes = ballotPaperCount;
			}

			return transferVotes;
		}
        
		public virtual void QueueDistributionOf1stPreferences()
		{
			AddDistributionToQueue(DistributionType.Initial, null, BallotPapers);
		}

		public virtual void QueueSurplusDistribution(MCandidate candidate)
		{
			List<BallotPaperBL> bps = GetCandidateBallotPapers(candidate, BallotPapers);
			List<BallotPaperBL> excludeList = new List<BallotPaperBL>();

			foreach (DistributionBL d in DistributionQueue)
			{
				excludeList.AddRange(d.BallotPapers);
			}

			AddDistributionToQueue(DistributionType.Surplus, candidate, bps.Except(excludeList).ToList());
		}

		public virtual void QueueSurplusDistributions(List<MCandidate> candidatesToQueue)
		{
			foreach (MCandidate candidateToQueue in candidatesToQueue)
			{
				QueueSurplusDistribution(candidateToQueue);
			}
		}

		public virtual void AddDistributionToQueue(DistributionType distType, MCandidate candidate, List<BallotPaperBL> paperstotransfer)
		{
			int lastQueuedStepNumber = DistributionQueue.Count;
			
			if (lastQueuedStepNumber > 0)
			{
				lastQueuedStepNumber = DistributionQueue.Max(x => x.Step);
			}
			lastQueuedStepNumber++;

			int countOfPapersToTransfer = CountPapers(paperstotransfer);

			if (distType == DistributionType.Exclusion &&
					(CalculationMethod == CalculationMethod.Proportional || CalculationMethod == CalculationMethod.Countback))
			{
				List<decimal> transfervalues = paperstotransfer.Select(x => x.WeightedTransferValue).Distinct().ToList();

				List<decimal> tvsOrderDesc = new List<decimal>();

				if (transfervalues.Any(x => x == 0))
				{
					tvsOrderDesc.Add(0);
				};
				tvsOrderDesc.AddRange((from tv in transfervalues where tv != 0 orderby tv descending select tv).ToList());

				foreach (decimal transfervalue in tvsOrderDesc)
				{
					List<BallotPaperBL> parcel = GetCandidateBallotPapersReceivedAtTransferValue(candidate, paperstotransfer, transfervalue);
					int countOfPapersInParcel = CountPapers(parcel);
					int votesTransferring = DetermineTotalVotesForTransfer(candidate, distType, countOfPapersInParcel, transfervalue);

					if (parcel.Count > 0)
					{
						QueueDistribution(parcel, candidate, distType, lastQueuedStepNumber, transfervalue, votesTransferring);

						lastQueuedStepNumber++;
					}
				}

				if (tvsOrderDesc.Count == 0)
				{
					QueueDistribution(new List<BallotPaperBL>(), candidate, distType, lastQueuedStepNumber, 0, 0);
				}

			}
			else
			{
				if (distType == DistributionType.Surplus)
				{
					if ((GetCandidateProgressTotalVotes(candidate) - Quota) > 0)
					{
						int numerator = 0;
						int denominator = 0;
						decimal transferValue = 0;
						GetCandidateTransferValue(candidate, paperstotransfer, ref transferValue, ref numerator, ref denominator);
						int votesTransferring = DetermineTotalVotesForTransfer(candidate, distType, countOfPapersToTransfer, transferValue);
						QueueDistribution(paperstotransfer, candidate, distType, lastQueuedStepNumber, transferValue, votesTransferring);
					}

					else
					{
						foreach (BallotPaperBL bp in paperstotransfer)
						{
							bp.ImmovablePosition = candidate.Position;
						}
					}

				}
				else
				{
					QueueDistribution(paperstotransfer, candidate, distType, lastQueuedStepNumber, 1, countOfPapersToTransfer);
				}
			}
		}


		public virtual bool IsQueuedDistributions
		{
			get
			{
				return DistributionQueue.Count() > 0;
			}
		}

		public virtual void RunNextQueuedDistribution()
		{
			if (IsQueuedDistributions)
			{
				DistributionBL nextQueuedDistribution = (from fd in DistributionQueue
																								 orderby fd.Step ascending
																								 select fd).FirstOrDefault();

				Distribute(nextQueuedDistribution.Candidate, nextQueuedDistribution.BallotPapers, nextQueuedDistribution.DistributionType, nextQueuedDistribution.Transfers[0].WeightedTransferValue, nextQueuedDistribution.Transfers[0].VotesTotal);

				if (nextQueuedDistribution.DistributionType == DistributionType.Initial)
				{
					ExcludedCandidates.ForEach(x => SetCandidateCountStatus(x, CandidateCountStatus.NotParticipating));
				}

				DistributionQueue.Remove(nextQueuedDistribution);
			}
		}

		protected virtual void QueueDistribution(List<BallotPaperBL> papers, MCandidate candidate, DistributionType distType, int step, decimal transferValue, int totalVotes)
		{
			DistributionBL dbl = new DistributionBL
			{
				BallotPapers = papers,
				DistributionType = distType,
				Candidate = candidate,
				Step = step,
				Transfers = new List<MTransfer>()
			};
			dbl.Transfers.Add(new MTransfer { WeightedTransferValue = transferValue, VotesTotal = totalVotes });
			DistributionQueue.Add(dbl);
		}

		public virtual List<MCandidate> ResolvePRExclusionTie(List<MCandidate> tiedCandidatesToResolve)
		{
			List<MCandidate> resolvedCandidates = new List<MCandidate>(tiedCandidatesToResolve);
			if (this.TieResolutionLogic == TieResolutionLogic.CompareBack_ByLot)
			{
				foreach (MDistribution d in Distributions.OrderByDescending(x => x.Step))
				{
					if (resolvedCandidates.Count > 1)
					{
						foreach (MTransfer t in d.Transfers.OrderByDescending(x => x.CycleNo))
						{
							var ca = (from cas in t.CandidateAllocations
												from tied in resolvedCandidates
												where cas.Candidate.Position == tied.Position
												orderby cas.TotalVotes ascending
												select new { tied.Position, cas.TotalVotes });

							List<int> distinctTotalVotes = (from c in ca
																							select c.TotalVotes).Distinct().ToList();

							if (distinctTotalVotes.Count() > 1)
							{
								int lowestTotal = distinctTotalVotes.OrderBy(x => x).First();

								var lowestTotalCandidates = (from c in ca
																						 where c.TotalVotes == lowestTotal
																						 select c).ToList();

								List<MCandidate> candidatesToRemove = (from rc in resolvedCandidates
																											 where !lowestTotalCandidates.Exists(x => x.Position == rc.Position)
																											 select rc).ToList();

								foreach (MCandidate c in candidatesToRemove)
								{
									resolvedCandidates.Remove(c);
								}
							}
						}
					}
				}
			}

			if (resolvedCandidates.Count > 1)
			{
				int randomIndex = RandomHelper.RandomNumberBetweenMinAndMaxInclusive(0, resolvedCandidates.Count - 1);
				MCandidate randomlyPickedCandidate = resolvedCandidates[randomIndex];
				resolvedCandidates.Clear();
				resolvedCandidates.Add(randomlyPickedCandidate);
			}

			return resolvedCandidates;
		}

		public virtual void ExcludeCandidate(MCandidate candidate)
		{
			List<BallotPaperBL> bps = GetCandidateBallotPapers(candidate, BallotPapers);
			AddDistributionToQueue(DistributionType.Exclusion, candidate, bps);
			AddExcludedCandidate(candidate);
		}
		
		public virtual void ExcludeCandidateWithoutDistribution(MCandidate candidate)
		{
			AddExcludedCandidate(candidate);
		}

		public virtual void AddExcludedCandidate(MCandidate candidate)
		{
			if (!ExcludedCandidates.Any(x => x.Position == candidate.Position))
			{
				ExcludedCandidates.Add(candidate);
				SetCandidateCountStatus(candidate, CandidateCountStatus.Excluded);
			}
			else
			{
				throw new Exception(string.Format("Candidate {0} has already been excluded.", candidate.Name));
			}
		}

		public int Select0Or1Randomly()
		{
			return RandomHelper.RandomNumberBetweenMinAndMaxInclusive(0, 1);
		}

		public virtual void ElectCandidate(MCandidate candidate)
		{
			AddElectedCandidate(candidate);
		}

		protected virtual void AddElectedCandidate(MCandidate candidate)
		{
			if (!ElectedCandidates.Any(x => x.Position == candidate.Position))
			{
				ElectedCandidates.Add(candidate);
				SetCandidateCountStatus(candidate, CandidateCountStatus.Elected);
			}
			else
			{
				throw new Exception(string.Format("Candaidate {0} has already been elected.", candidate.Name));
			}
		}
		
		public List<MCandidate> ResolvePRSurplusTie(List<MCandidateEx> tiedCandidatesToResolve)
		{
			List<MCandidate> currentlyTiedCandidates = new List<MCandidate>(tiedCandidatesToResolve.Select(x => new MCandidate { Name = x.Name, Position = x.Position }).ToList());

			List<MCandidate> returnlist = new List<MCandidate>();
			do
			{
				if (currentlyTiedCandidates.Count == 1)
				{
					returnlist.Add(currentlyTiedCandidates[0]);
				}
				else
				{
					List<MCandidate> resolvedCandidates = GetHighestDifferenceInCycle(currentlyTiedCandidates);
					if (resolvedCandidates.Count == 1)
					{
						returnlist.Add(resolvedCandidates[0]);
					}
					else
					{
						int resolvedCount = resolvedCandidates.Count;
						int randomIndex = RandomHelper.RandomNumberBetweenMinAndMaxInclusive(0, resolvedCount - 1);
						returnlist.Add(resolvedCandidates[randomIndex]);
					}

					foreach (MCandidate c in returnlist)
					{
						currentlyTiedCandidates.Remove(c);
					}
				}
			} while (returnlist.Count < tiedCandidatesToResolve.Count);

			return returnlist;
		}

		protected virtual void Save()
		{
			MCalculation currentcalc = new MCalculation
			{
				CalculationId = CalculationId,
				ElectionElectorateVacancyId = _electionElectorateVacancyId,
				Quota = Quota,
				TotalBallotPapers = CountPapers(BallotPapers) + InformalPapersTotal,
				TotalFormalBallotPapers = CountPapers(BallotPapers),
				TotalInformalBallotPapers = InformalPapersTotal,
				Vacancies = _Vacancies,
				CalculateDateTime = DateTime.Now,
				CalculationState = CalculationStatus,
				CalculationMethod = CalculationMethod,
				CalculatedByComputer = true
			};

			_computerCountDataContext.SaveCalculation(currentcalc, Distributions, _electionElectorateVacancyId);

			if (currentcalc.CalculationState == CalculateState.Finished)
			{
				_reporting.SaveCalculation(currentcalc, Distributions);
			}
		}	
		
    }

    public class CalculateBL : Calculate
    {
        protected string _ConnectionString;              

        public CalculateBL(IComputerCountDataContext dataContext, IReportingDAL reporting, Guid ElectionElectorateVacancyId)
          : base(dataContext, reporting, ElectionElectorateVacancyId)
        {
            Initialise();
        }

        public CalculateBL(Guid ElectionElectorateVacancyId, int Vacancies, CalculationMethod CalcMethod, TieMethodType TieMethod, TieResolutionLogic TieResolutionLogic, string ExcludedCandidates, IComputerCountDataContext dataContext, IReportingDAL reporting)
            : this(dataContext, reporting, ElectionElectorateVacancyId)
        {
            _Vacancies = Vacancies;
            TieMethodType = TieMethod;
            CalculationMethod = CalcMethod;
            this.InitialExcludedCandidates = string.IsNullOrWhiteSpace(ExcludedCandidates) ? new List<MCandidate>() :
                ExcludedCandidates.Split(',').Select(x => new MCandidate { Name = "", Position = int.Parse(x) }).ToList();
            this.TieResolutionLogic = TieResolutionLogic;
        }

        private void Initialise()
        {
            InformalPapersTotal = 0;
            ElectedCandidates = new List<MCandidate>();
            ExcludedCandidates = new List<MCandidate>();
            InitialExcludedCandidates = new List<MCandidate>();
            Candidates = new List<MCandidate>();
            Distributions = new List<MDistribution>();
            DistributionQueue = new List<DistributionBL>();
        }       

    }

    public class BallotPaperBL : MBallotPaper, IBallotPaperBL
    {
        private bool _immovable = false;
        private int _immovablePosition = 0;
        private int _frequency = 1;

        public BallotPaperBL() : base()
        {

        }

        public new int Frequency
        {
            get
            {
                return _frequency;
            }
            set
            {
                _frequency = value;
            }
        }

        public int ImmovablePosition
        {
            get
            {
                return _immovablePosition;
            }
            set
            {
                if (_immovable == false)
                {
                    _immovablePosition = value;
                    _immovable = true;
                }
            }
        }

        public void ResetImmovablePosition()
        {
            _immovable = false;
            _immovablePosition = 0;
        }

        public new int ExhaustAfter
        {
            get
            {
                if (!base.ExhaustAfter.HasValue)
                {
                    return base.Entries.Count;
                }
                else
                {
                    return base.ExhaustAfter.Value;
                }
            }
            set
            {
                base.ExhaustAfter = value;
            }

        }
        
        public int GetNextPreferencePosition(List<MCandidate> electedCandidates, List<MCandidate> ExcludedCandidates)
        {
            var excluded = new List<MCandidate>();
            if (electedCandidates != null)
            {
                excluded.AddRange(electedCandidates);
            }
            if (ExcludedCandidates != null)
            {
                excluded.AddRange(ExcludedCandidates);
            }

            return GetPreferencePosition(excluded);
        }
       
        public int CurrentPreferencePosition(List<MCandidate> electedCandidates, List<MCandidate> ExcludedCandidates)
        {
            var excluded = new List<MCandidate>();
            if (electedCandidates != null)
            {
                excluded.AddRange(electedCandidates);
            }
            if (ExcludedCandidates != null)
            {
                excluded.AddRange(ExcludedCandidates);
            }

            return GetPreferencePosition(excluded);
        }

        public int GetPreferencePosition(List<MCandidate> electedAndExcludedCandidates)
        {
            int bpes = _immovablePosition;

            if (!_immovable)
            {
                bpes = (from bpe in Entries
                        where bpe.BpValue.HasValue
                                && electedAndExcludedCandidates.All(x => x.Position != bpe.Candidate.Position)
                                && bpe.BpValue.Value <= ExhaustAfter
                        orderby bpe.BpValue ascending
                        select bpe.Candidate.Position).FirstOrDefault();
            }

            return bpes;

        }
    }

    public interface IBallotPaperBL
    {
        int BallotPaperId { get; set; }
        int BatchId { get; set; }
        int CycleNo { get; set; }
        List<MBallotPaperEntry> Entries { get; set; }
        int ExhaustAfter { get; set; }
        decimal WeightedTransferValue { get; set; }
        BallotPaperStatus Status { get; set; }
        VoteType VoteType { get; set; }
        int BallotPaperNbr { get; set; }
        bool FormalFlag { get; set; }
        int Frequency { get; set; }
        int ImmovablePosition { get; set; }
    }

    internal static class RandomHelper
    {
        private static readonly Random random = new Random();
        private static readonly object syncLock = new object();

        internal static int RandomNumberBetweenMinAndMaxInclusive(int min, int max)
        {
            lock (syncLock)
            {
               return random.Next(min, max + 1);
            }
        }
    }
}
