using LinqToDB;
using LinqToDB.Data;
using System.Data;
using Vec.CalculationEngine.Data;
using Vec.Common.Entities;
using Vec.ComputerCount.Data.ResultsDb;
using Vec.ComputerCount.Entities;

namespace Vec.ComputerCount.DAL;

public enum TieResolutionLogic
{
    ByLot = 0,
    CompareBack_ByLot = 1
}

public interface IComputerCountDataContext
{
	CalculationQueue? GetCalculationQueue(Guid calculationId);
	void SaveCalculation(MCalculation calc, List<MDistribution> distributions, Guid electionElectorateVacancyId);
	MCalculation GetCalculation(Guid ElectionElectorateVacancyId, Guid CalculationId);
	List<MDistribution> GetAllDistributions(Guid CalculationId);
	List<MBallotPaper> LoadBallotPapersWithFrequency(Guid electionElectorateVacancyId);
	List<MAllocatedTicket> GetAllocatedTickets(Guid electionElectorateVacancyId);
	bool AllElectionElectorateVacanciesHaveCountParameterWithValue(IEnumerable<Guid> electionElectorateVacancyIds, string countParameterName, string value);
	Common.Entities.CountType GetManualResultsComputerCountSource(Guid electionElectorateVacancyId, bool effective);
	MRound GetLastRound(Guid electionElectorateVacancyId, Vec.Common.Entities.CountPhase? countPhaseFilter = null);
	GetATLVotesSummaryResponse.ManualResultsEntity GetComputerCountSourceResults(Guid electionElectorateVacancyId, IEnumerable<int> ballotBoxIds);
	GetInformalBatchesSummaryResponse.ManualResultsEntity GetComputerCountSourceInformal(Guid electionElectorateVacancyId, IEnumerable<int> ballotBoxIds);
	ATLDataEntryType GetATLDataEntryType(Guid electionElectorateVacancyId);
	InformalDataEntryType GetInformalDataEntryType(Guid electionElectorateVacancyId);
	int GetInformalTotalVotes(Guid electionElectorateVacancyId);

}

public class DComputerCount : IComputerCountDataContext
{
	readonly IBallotPaperFetcher _ballotPaperFetcher;
	protected readonly ResultsDatabase _database;
	
	public DComputerCount(IDataContextFactory<ResultsDatabase> factory, IBallotPaperFetcher ballotPaperFetcher)
	{
		_database = factory.CreateDataContext();
	
		_ballotPaperFetcher = ballotPaperFetcher;
	}

	public CalculationQueue? GetCalculationQueue(Guid calculationId)
	{
		return _database.CalculationQueue.FirstOrDefault(x => x.CalculationId == calculationId);
	}

	public void SaveCalculation(MCalculation calc, List<MDistribution> distributions, Guid electionElectorateVacancyId)
	{
		InsertOrMergeCalculation(calc, distributions, electionElectorateVacancyId);
	}

	protected void InsertOrMergeCalculation(MCalculation calc, List<MDistribution> distributions, Guid electionElectorateVacancyId)
	{
		List<Calculation> existingCalculation = (from c in _database.Calculation
																						 where c.CalculationId == calc.CalculationId
																						 select c).ToList();

		if (existingCalculation.Count == 0)
		{
			InsertCalculation(calc, distributions, electionElectorateVacancyId);
		}
		else
		{
			MergeCalculation(existingCalculation[0], calc, distributions);
		}
	}

	protected void MergeCalculation(Calculation savedCalculation, MCalculation calc, List<MDistribution> distributions)
	{
		int lastSavedCycle = 0;
		int lastSavedStep = 0;

		if (savedCalculation.CalculationStateId != (int)calc.CalculationState)
		{
			savedCalculation.CalculationStateId = (int)calc.CalculationState;

			_database.Update(savedCalculation);
		}

		var savedDistributions = _database.Distribution.Where(x => x.CalculationId == savedCalculation.CalculationId).ToList();

		if (savedDistributions.Count > 0)
		{
			Distribution lastSavedDistribution = (from d in savedDistributions
																						orderby d.Step descending
																						select d).FirstOrDefault();

			lastSavedStep = lastSavedDistribution.Step;

			Transfer lastSavedTransfer = _database.Transfer
				.Where(x => x.DistributionId == lastSavedDistribution.DistributionId)
				.OrderByDescending(x => x.CycleNumber)
				.FirstOrDefault();

			lastSavedCycle = (int)lastSavedTransfer.CycleNumber;
			MergeTransfer(lastSavedTransfer, lastSavedStep, distributions);
		}
		InsertNewTransfers(savedCalculation, distributions, lastSavedStep, lastSavedCycle);
	}

	protected void MergeTransfer(Transfer dbt, int lastSavedStep, List<MDistribution> distributions)
	{
		MTransfer latestVersionOfTransfer = (from t in distributions.Where(x => x.Step == lastSavedStep).First().Transfers
																				 where t.CycleNo == dbt.CycleNumber
																				 select t).FirstOrDefault();

		foreach (CandidateAllocation dbca in _database.CandidateAllocation.Where(x => x.TransferId == dbt.TransferId).ToList())
		{
			MCandidateAllocation ca = latestVersionOfTransfer.CandidateAllocations.Where(x => x.Candidate.Position == dbca.CandidatePosition).FirstOrDefault();
			if ((int)ca.CountStatus != dbca.CandidateCountStatusId)
			{
				dbca.CandidateCountStatusId = (int)ca.CountStatus;
				dbca.ElectorOrExcludeOrder = ca.ElectorOrExcludeOrder;

				_database.Update(dbca);
			}
		}
	}

	protected void InsertCalculation(MCalculation calculation, List<MDistribution> distributions, Guid electionElectorateVacancyId)
	{
		var recountconfig = (from p in _database.CountParameter
												 where p.ElectionElectorateVacancyId == electionElectorateVacancyId
												 && p.Name == Vec.Common.Entities.Consts.PARAM_HasRecount
												 && (p.Value == Vec.Common.Entities.RecountEnum.WithValidation.ToString()
														 || p.Value == Vec.Common.Entities.RecountEnum.WithoutValidation.ToString())
												 select p.Value);

		var counttype = Vec.Common.Entities.CountType.Primary;
		if (recountconfig.Count() > 0)
		{
			counttype = Vec.Common.Entities.CountType.Recount;
		}

		Calculation newcalc = new Calculation
		{
			CalculationId = calculation.CalculationId,
			CalculationStateId = (int)calculation.CalculationState,
			ElectionElectorateVacancyId = calculation.ElectionElectorateVacancyId,
			CalculateDateTime = calculation.CalculateDateTime,
			Quota = calculation.Quota,
			TotalBallotPapers = calculation.TotalBallotPapers,
			TotalFormalBallotPapers = calculation.TotalFormalBallotPapers,
			TotalInformalBallotPapers = calculation.TotalInformalBallotPapers,
			Vacancies = calculation.Vacancies,
			CalculatedByComputer = calculation.CalculatedByComputer,
			CalculationMethodId = (int)calculation.CalculationMethod,
			CountTypeId = (int)counttype
		};

		_database.Insert(newcalc);

		InsertNewTransfers(newcalc, distributions, 0, 0);
	}

	protected void InsertNewTransfers(Calculation savedCalculation, List<MDistribution> distributions, int lastSavedStep, int lastSavedCycle)
	{
		var saveDistributions = new List<Distribution>();
		var saveTransfers = new List<Transfer>();
		var saveCandidateAllocations = new List<CandidateAllocation>();

		foreach (MDistribution d in distributions)
		{
			Distribution currentDistribution;

			if (d.Step > lastSavedStep)
			{
				int? candidatePosition = null;
				if (d.Candidate != null)
				{
					candidatePosition = d.Candidate.Position;
				}

				currentDistribution = new Distribution
				{
					CalculationId = savedCalculation.CalculationId,
					DistributionId = Guid.NewGuid(),
					CandidatePosition = candidatePosition,
					DistributionTypeId = (int)d.DistributionType,
					Step = d.Step
				};

				saveDistributions.Add(currentDistribution);
			}
			else
			{
				currentDistribution = (from dbd in _database.Distribution
															 where dbd.Step == d.Step && dbd.CalculationId == savedCalculation.CalculationId
															 select dbd).FirstOrDefault();
			}

			foreach (MTransfer t in d.Transfers)
			{
				if (t.CycleNo > lastSavedCycle)
				{
					Transfer newtransfer = Translate(t, currentDistribution.DistributionId);

					saveTransfers.Add(newtransfer);

					foreach (MCandidateAllocation ca in t.CandidateAllocations)
					{
						saveCandidateAllocations.Add(Translate(ca, newtransfer.TransferId));
					}
				}
			}
		}

		if (saveDistributions.Any())
		{
			_database.Distribution.BulkCopy(saveDistributions);
		}
		if (saveTransfers.Any())
		{
			_database.Transfer.BulkCopy(saveTransfers);
		}
		if (saveCandidateAllocations.Any())
		{
			_database.CandidateAllocation.BulkCopy(saveCandidateAllocations);
		}
	}	

	public MCalculation GetCalculation(Guid electionElectorateVacancyId, Guid calculationId)
	{
		var calc = (from c in _database.Calculation
								where c.ElectionElectorateVacancyId == electionElectorateVacancyId &&
								c.CalculationId == calculationId
								select c).FirstOrDefault();

		if (calc != null)
		{
			return Translate(calc, electionElectorateVacancyId);
		}

		return null;
	}

	private MCalculation Translate(Calculation c, Guid electionElectorateVacancyId)
	{
		return new MCalculation
		{
			CalculationMethod = (Vec.Common.Entities.CalculationMethod)(c.CalculationMethodId ?? 2),
			CalculateDateTime = c.CalculateDateTime,
			CalculationId = c.CalculationId,
			ElectionElectorateVacancyId = electionElectorateVacancyId,
			Quota = c.Quota,
			TotalBallotPapers = c.TotalBallotPapers,
			TotalFormalBallotPapers = c.TotalFormalBallotPapers,
			TotalInformalBallotPapers = c.TotalInformalBallotPapers,
			CalculatedByComputer = c.CalculatedByComputer,
			CountType = c.CountTypeId.HasValue ? (Common.Entities.CountType)c.CountTypeId.Value : Common.Entities.CountType.Unknown,
			Vacancies = c.Vacancies
		};
	}

	protected MTransfer Translate(Transfer t)
	{
		MTransfer returntransfer = new MTransfer
		{
			CycleNo = (int)t.CycleNumber,
			ExhaustedPapers = (int)t.ExhaustedPapers,
			ExhaustedVotes = (int)t.ExhaustedVotes,
			FromCycles = (string)t.FromCycles,
			GainLoss = (int)t.GainLoss,
			TotalExhaustedPapers = (int)t.ProgressTotalExhaustedPapers,
			TotalExhaustedVotes = (int)t.ProgressTotalExhaustedVotes,
			TotalGainLoss = (int)t.ProgressTotalGainLoss,
			BallotPapersTotal = (int)t.TotalBallotPapers,
			WeightedTransferValue = (decimal)t.WeightedTransferValue,
			CandidateAllocations = new List<MCandidateAllocation>()
		};

		foreach (CandidateAllocation ca in t.CandidateAllocations)
		{
			returntransfer.CandidateAllocations.Add(new MCandidateAllocation
			{
				CountStatus = (Vec.ComputerCount.Entities.CandidateCountStatus)ca.CandidateCountStatusId,
				Candidate = new MCandidate { Name = string.Format("C{0}", ca.CandidatePosition), Position = ca.CandidatePosition },
				ElectorOrExcludeOrder = (int)ca.ElectorOrExcludeOrder,
				BallotPapers = (int)ca.BallotPapers,
				TotalBallotPapers = (int)ca.ProgressTotalBallotPapers,
				TotalVotes = (int)ca.ProgressTotalVotes,
				Votes = (int)ca.Votes
			});
		}

		return returntransfer;
	}

	public List<MDistribution> GetAllDistributions(Guid CalculationId)
	{
		List<Distribution> distributions = (from c in _database.Calculation
																				join d in _database.Distribution on c.CalculationId equals d.CalculationId
																				where c.CalculationId == CalculationId
																				orderby d.Step
																				select d).ToList();
		
		List<MDistribution> returndistributions = new List<MDistribution>();
		foreach (Distribution d in distributions)
		{
			returndistributions.Add(Translate(d));
		}

		return returndistributions;
	}
    
	public List<MBallotPaper> LoadBallotPapersWithFrequency(Guid electionElectorateVacancyId)
	{
		var uniquePaperPatternsWithFrequency = _ballotPaperFetcher.GetBallotPapers(electionElectorateVacancyId);

		var bps = new List<MBallotPaper>();

		foreach (var bp in uniquePaperPatternsWithFrequency)
		{
			var bpelist = new List<MBallotPaperEntry>();
			int position = 1;

			foreach (var x in bp.PreferencePattern.TrimStart(',').Split(','))
			{
				bpelist.Add(new MBallotPaperEntry
				{
					Mark = x == "-1" ? "" : x,
					BpValue = x == "-1" ? null : (int?)int.Parse(x),
					Candidate = new MCandidate { Position = position, Name = position.ToString() }
				});
				position++;
			}

			var mbp = new MBallotPaper
			{
				BallotPaperId = bp.BallotPaperId,
				BallotPaperNbr = bp.BallotPaperNbr.Value,
				BatchId = bp.BatchId,
				CycleNo = 0,
				Entries = bpelist,
				FormalFlag = bp.FormalFlag.Value,
				ExhaustAfter = bp.ExhaustAfter,
				Status = (Vec.Common.Entities.BallotPaperStatus)bp.BallotPaperStatusId.Value,
				WeightedTransferValue = bp.WeightedTransferValue ?? 0,
				Frequency = bp.Frequency
			};

			bps.Add(mbp);
		}

		return bps.ToList();
	}

	public List<MAllocatedTicket> GetAllocatedTickets(Guid electionElectorateVacancyId)
	{
		var currentRound = GetLastRound(electionElectorateVacancyId);

		List<MAllocatedTicket> result = new List<MAllocatedTicket>();
		result.AddRange(from at in _database.AllocatedTicket
										where at.ElectionElectorateVacancyId == electionElectorateVacancyId && at.CountPhaseId == (int)currentRound.CountPhase
										orderby at.GroupId, at.TicketNumber
										select new MAllocatedTicket
										{
											AllocatedTicketId = at.AllocatedTicketId,
											GroupId = at.GroupId,
											GroupName = at.GroupName,
											TicketNumber = at.TicketNumber,
											Votes = at.Votes,
											BatchNbr = at.BatchNbr,
											TimeStamp = at.Timestamp,
											Preferences = (from tp in _database.TicketPreference
																		 where tp.ElectionElectorateVacancyId == electionElectorateVacancyId
																		 && tp.AllocatedTicketId == at.AllocatedTicketId
																		 orderby tp.Order
																		 select new MTicketPreference
																		 {
																			 TicketPreferenceId = tp.TicketPreferenceId,
																			 Order = tp.Order,
																			 Value = tp.Value,
																			 TimeStamp = tp.Timestamp
																		 }).ToList()
										});
		
		return result;
	}

	public Dictionary<int, int> GetGroupATLTotalVotes(Guid electionElectorateVacancyId)
	{
		var atlDataEntryType = GetATLDataEntryType(electionElectorateVacancyId);
		var currentRound = GetLastRound(electionElectorateVacancyId);

		var result = (from b in _database.BallotBox
									join bt in _database.BallotBoxTicket
									on b.BallotBoxId equals bt.BallotBoxId
									where b.ElectionElectorateVacancyId == electionElectorateVacancyId
									where
									(
											 (atlDataEntryType == ATLDataEntryType.Absolute && (Vec.Common.Entities.CountPhase)bt.CountPhaseId == currentRound.CountPhase) ||
											 (atlDataEntryType == ATLDataEntryType.Adjusted)
									)
									select new { GroupId = bt.GroupId, Total = bt.Total })
									.ToArray() 
									.GroupBy(bt => bt.GroupId)
									.Select(g => new { GroupId = g.Key, GetGroupATLTotalVotes = (int)((decimal?)g.Sum(ticket => ticket.Total) ?? 0) })
									.ToDictionary(x => x.GroupId, x => x.GetGroupATLTotalVotes);
		
		if (atlDataEntryType == ATLDataEntryType.Adjusted)
		{
			var computerCountSourceResults = GetComputerCountSourceResults(electionElectorateVacancyId);
			if (computerCountSourceResults.CountType != Common.Entities.CountType.Unknown)
			{
				foreach (var bbr in computerCountSourceResults.BallotBoxResults)
				{
					foreach (var bbcr in bbr.BallotBoxCandidateResults.Where(x => x.GroupId.HasValue && x.ATLVotes.HasValue))
					{
						if (result.ContainsKey(bbcr.GroupId.Value))
						{
							result[bbcr.GroupId.Value] += bbcr.ATLVotes.Value;
						}
						else
						{
							result[bbcr.GroupId.Value] = bbcr.ATLVotes.Value;
						}
					}
				}
			}
		}

		return result;
	}

	public int GetInformalTotalVotes(Guid electionElectorateVacancyId)
	{
		var informalDataEntryType = GetInformalDataEntryType(electionElectorateVacancyId);
		var currentRound = GetLastRound(electionElectorateVacancyId);

		var result = (from b in _database.BallotBox
									join bbi in _database.BallotBoxInformal
									on b.BallotBoxId equals bbi.BallotBoxId
									where b.ElectionElectorateVacancyId == electionElectorateVacancyId
									where
									(
											 (informalDataEntryType == InformalDataEntryType.Absolute && (Vec.Common.Entities.CountPhase)bbi.CountPhaseId == currentRound.CountPhase) ||
											 (informalDataEntryType == InformalDataEntryType.Adjusted)
									)
									select new { BallotBoxId = bbi.BallotBoxId, Total = bbi.Total })
									.ToArray() 
									.GroupBy(x => x.BallotBoxId)
									.ToDictionary(k => k.Key, v => v.Sum(x => x.Total));
		
		if (informalDataEntryType == InformalDataEntryType.Adjusted)
		{			
			var computerCountSourceResults = GetComputerCountSourceInformal(electionElectorateVacancyId, null);
			if (computerCountSourceResults.CountType != Common.Entities.CountType.Unknown)
			{
				foreach (var bbr in computerCountSourceResults.BallotBoxResults.Where(x => x.InformalVotes.HasValue))
				{
					if (result.ContainsKey(bbr.BallotBoxId))
					{
						result[bbr.BallotBoxId] += bbr.InformalVotes.Value;
					}
					else
					{
						result[bbr.BallotBoxId] = bbr.InformalVotes.Value;
					}
				}
			}
		}

		return result.Sum(x => x.Value);
	}
	
	public Common.Entities.CountType GetManualResultsComputerCountSource(Guid electionElectorateVacancyId, bool effective)
	{
		var currentRound = GetLastRound(electionElectorateVacancyId);
		if (currentRound.CountPhase == Vec.Common.Entities.CountPhase.Initial || effective == false)
		{
			var computerCountSourceCountParameter = GetComputerCountSourceCountParameter(electionElectorateVacancyId);

			return computerCountSourceCountParameter;
		}

		if (currentRound.CountPhase == Vec.Common.Entities.CountPhase.Recount)
		{
			var initialRound = GetLastRound(electionElectorateVacancyId, Vec.Common.Entities.CountPhase.Initial);
			if (initialRound == null || initialRound.ComputerCountSource.HasValue == false || initialRound.ComputerCountSource.Value == Common.Entities.CountType.Unknown)
			{				
				return Common.Entities.CountType.Unknown;
			}

			return initialRound.ComputerCountSource.Value;
		}

		return Common.Entities.CountType.Unknown;
	}

	private Common.Entities.CountType GetComputerCountSourceCountParameter(Guid electionElectorateVacancyId)
	{
		var computerCountType = (from p in _database.CountParameter
														 where p.Name == Consts.PARAM_ComputerCountSource
																	 && p.ElectionElectorateVacancyId == electionElectorateVacancyId
														 select p.Value).FirstOrDefault();

		var countType = Vec.Common.Entities.CountType.Unknown;

		if (computerCountType != null)
			Enum.TryParse(computerCountType, out countType);

		return countType;
	}

	public Vec.ComputerCount.Entities.MRound GetLastRound(Guid electionElectorateVacancyId, Vec.Common.Entities.CountPhase? countPhaseFilter = null)
	{
		var currentRound = GetLastDALRound(electionElectorateVacancyId, countPhaseFilter);
		if (currentRound != null)
		{
			return new Vec.ComputerCount.Entities.MRound
			{
				ElectionElectorateVacancyId = electionElectorateVacancyId,
				CountPhase = (Vec.Common.Entities.CountPhase)currentRound.CountPhaseId,
				CountNumber = currentRound.CountNumber,
				ComputerCountSource = (currentRound.ComputerCountSourceCountTypeId.HasValue) ? (Common.Entities.CountType?)currentRound.ComputerCountSourceCountTypeId : null
			};
		}

		return new Vec.ComputerCount.Entities.MRound
		{
			ElectionElectorateVacancyId = electionElectorateVacancyId,
			CountPhase = Vec.Common.Entities.CountPhase.Initial,
			CountNumber = 1,
			ComputerCountSource = GetComputerCountSourceCountParameter(electionElectorateVacancyId)
		};
	}

	private Round GetLastDALRound(Guid electionElectorateVacancyId, Vec.Common.Entities.CountPhase? countPhaseFilter)
	{
		var rounds = _database.Round
			.Where(x => x.ElectionElectorateVacancyId == electionElectorateVacancyId)
			.ToList();

		var currentRound = rounds.Where(x => countPhaseFilter.HasValue == false || x.CountPhaseId == (int)countPhaseFilter)
															.OrderBy(x => x.CountPhaseId)
															.ThenBy(x => x.CountNumber)
															.LastOrDefault();

		return currentRound;
	}

	public GetATLVotesSummaryResponse.ManualResultsEntity GetComputerCountSourceResults(Guid electionElectorateVacancyId, IEnumerable<int> ballotBoxIds = null)
	{
		var result = new GetATLVotesSummaryResponse.ManualResultsEntity();
		var manualResultsComputerCountSource = GetManualResultsComputerCountSource(electionElectorateVacancyId, effective: true);
		result.CountType = manualResultsComputerCountSource;

		if (manualResultsComputerCountSource == Common.Entities.CountType.Unknown)
		{
			return result;
		}

		var ballotBoxCandidateResultsQuery =
				from bbcr in _database.BallotBoxCandidateResult
				join bbr in _database.BallotBoxResult on bbcr.BallotBoxResultId equals bbr.BallotBoxResultId
				join bb in _database.BallotBox on bbr.BallotBoxId equals bb.BallotBoxId
				where bbr.CountTypeId == (int)manualResultsComputerCountSource
				where bb.ElectionElectorateVacancyId == electionElectorateVacancyId
				select new
				{
					bbr.BallotBoxId,
					bbr.BallotBoxResultId,
					bbcr.BallotBoxCandidateResultId,
					bbcr.CandidatePosition,
					bbcr.GroupId,
					bbcr.AtlVotes,
					bbcr.BtlVotes,
				};
		
		if (ballotBoxIds != null)
		{
			ballotBoxCandidateResultsQuery = ballotBoxCandidateResultsQuery.Where(x => ballotBoxIds.Contains(x.BallotBoxId));
		}

		var ballotBoxCandidateResults = ballotBoxCandidateResultsQuery.ToList();
		var groupedBallotBoxCandidateResults = ballotBoxCandidateResults.GroupBy(y => y.BallotBoxResultId).ToList();

		result.BallotBoxResults =
				(from gbbcrs in groupedBallotBoxCandidateResults
				 let bbcr = gbbcrs.First()
				 select new GetATLVotesSummaryResponse.BallotBoxResultEntity
				 {
					 BallotBoxId = bbcr.BallotBoxId,
					 BallotBoxResultId = bbcr.BallotBoxResultId,
					 BallotBoxCandidateResults =
												gbbcrs.Select(x => new GetATLVotesSummaryResponse.BallotBoxCandidateResultEntity
												{
													BallotBoxCandidateResultId = x.BallotBoxCandidateResultId,
													BallotPaperPosition = x.CandidatePosition,
													GroupId = x.GroupId,
													ATLVotes = x.AtlVotes,
													BTLVotes = x.BtlVotes
												}).ToArray()
				 }).ToList();

		return result;
	}

	public GetInformalBatchesSummaryResponse.ManualResultsEntity GetComputerCountSourceInformal(Guid electionElectorateVacancyId, IEnumerable<int> ballotBoxIds)
	{
		var result = new GetInformalBatchesSummaryResponse.ManualResultsEntity();
		var manualResultsComputerCountSource = GetManualResultsComputerCountSource(electionElectorateVacancyId, effective: true);
		result.CountType = manualResultsComputerCountSource;

		if (manualResultsComputerCountSource == Common.Entities.CountType.Unknown)
		{
			return result;
		}

		var ballotBoxResultsQuery =
				from bb in _database.BallotBox
				join bbr in _database.BallotBoxResult on bb.BallotBoxId equals bbr.BallotBoxId
				where bbr.CountTypeId == (int)manualResultsComputerCountSource
				where bb.ElectionElectorateVacancyId == electionElectorateVacancyId
				select new
				{
					bb.BallotBoxId,
					bbr.BallotBoxResultId,
					bbr.InformalVotes,
				};
		
		if (ballotBoxIds != null)
		{
			ballotBoxResultsQuery = ballotBoxResultsQuery.Where(x => ballotBoxIds.Contains(x.BallotBoxId));
		}

		var ballotBoxResults = ballotBoxResultsQuery.ToList();

		result.BallotBoxResults =
				(from bbr in ballotBoxResults
				 select new GetInformalBatchesSummaryResponse.BallotBoxResultEntity
				 {
					 BallotBoxId = bbr.BallotBoxId,
					 BallotBoxResultId = bbr.BallotBoxResultId,
					 InformalVotes = bbr.InformalVotes
				 }).ToList();

		return result;
	}

	public ATLDataEntryType GetATLDataEntryType(Guid electionElectorateVacancyId)
	{
		var atlDataEntryType =
				(AllElectionElectorateVacanciesHaveCountParameterWithValue(new[] { electionElectorateVacancyId }, Consts.PARAM_AllowATLAdjustments, "true"))
						? ATLDataEntryType.Adjusted : ATLDataEntryType.Absolute;

		return atlDataEntryType;
	}

	public InformalDataEntryType GetInformalDataEntryType(Guid electionElectorateVacancyId)
	{
		var informalDataEntryType =
				(AllElectionElectorateVacanciesHaveCountParameterWithValue(new[] { electionElectorateVacancyId }, Consts.PARAM_AllowInformalAdjustments, "true"))
						? InformalDataEntryType.Adjusted : InformalDataEntryType.Absolute;

		return informalDataEntryType;
	}

	public bool AllElectionElectorateVacanciesHaveCountParameterWithValue(IEnumerable<Guid> electionElectorateVacancyIds, string countParameterName, string value)
	{
		var ids = electionElectorateVacancyIds.Cast<Guid?>();
		var countParametersWithGivenValue =
				_database.CountParameter
										.Where(x => ids.Contains(x.ElectionElectorateVacancyId))
										.Where(x => x.Name == countParameterName)
										.ToArray() 
										.Where(x => value.Equals(x.Value, StringComparison.OrdinalIgnoreCase))
										.ToArray();
		
		var result = (countParametersWithGivenValue.Length == electionElectorateVacancyIds.Count());

		return result;
	}

	protected MDistribution Translate(Distribution d)
	{
		MCandidate distCandidate = null;
		if (d.CandidatePosition != null)
		{
			distCandidate = new MCandidate { Name = string.Format("C{0}", (int)d.CandidatePosition), Position = (int)d.CandidatePosition };
		}

		MDistribution returndistribution = new MDistribution
		{
			Step = d.Step,
			DistributionType = (Vec.ComputerCount.Entities.DistributionType)d.DistributionTypeId,
			Candidate = distCandidate,
			Transfers = new List<MTransfer>()
		};

		foreach (Transfer t in d.Transfers)
		{
			returndistribution.Transfers.Add(Translate(t));
		};

		return returndistribution;
	}

	protected CandidateAllocation Translate(MCandidateAllocation ca, Guid parentId)
	{
		return new CandidateAllocation
		{
			TransferId = parentId,
			CandidateAllocationId = Guid.NewGuid(),
			CandidateCountStatusId = (int)ca.CountStatus,
			CandidatePosition = ca.Candidate.Position,
			ElectorOrExcludeOrder = ca.ElectorOrExcludeOrder,
			BallotPapers = ca.BallotPapers,
			ProgressTotalBallotPapers = ca.TotalBallotPapers,
			ProgressTotalVotes = ca.TotalVotes,
			Votes = ca.Votes
		};
	}

	protected Transfer Translate(MTransfer t, Guid parentId)
	{
		return new Transfer
		{
			DistributionId = parentId,
			TransferId = Guid.NewGuid(),
			CycleNumber = t.CycleNo,
			ExhaustedPapers = t.ExhaustedPapers,
			ExhaustedVotes = t.ExhaustedVotes,
			FromCycles = t.FromCycles,
			GainLoss = t.GainLoss,
			ProgressTotalExhaustedPapers = t.TotalExhaustedPapers,
			ProgressTotalExhaustedVotes = t.TotalExhaustedVotes,
			ProgressTotalGainLoss = t.TotalGainLoss,
			TotalBallotPapers = t.BallotPapersTotal,
			WeightedTransferValue = t.WeightedTransferValue
		};
	}	
	
}
