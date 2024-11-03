using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Vec.Calculation.BL.Workflow;
using Vec.CalculationEngine.DAL;
using Vec.CalculationEngine.DAL.Models;
using Vec.Common.Entities;
using Vec.ComputerCount.DAL;
using Vec.ComputerCount.DALReporting;
using Vec.ComputerCount.Entities;

namespace Vec.CalculationEngine.BL;

public interface ICalculationEngine
{
	Task Process(QueuedCalculation? queuedCalculation);
	Task Process(Guid calculationId);
}

public class CalculationEngine : ICalculationEngine
{
	readonly ICalculationContext _calculationContext;
	readonly IComputerCountDataContext _dataContext;
	readonly IReportingDAL _reportingDal;
	readonly ILogger<CalculationEngine> _logger;

	public CalculationEngine(
		IConfiguration configuration,
		ICalculationContext calculationContext,
		IComputerCountDataContext dataContext,
		IReportingDAL reportingDal,
		ILogger<CalculationEngine> logger
		)
	{
		_calculationContext = calculationContext;
		_dataContext = dataContext;
		_reportingDal = reportingDal;
		_logger = logger;
	}

	
	public async Task Process(Guid calculationId)
	{
		var calculationQueue = _dataContext.GetCalculationQueue(calculationId);
		if (calculationQueue != null)
		{
			await Process(new QueuedCalculation
			{
				CalculationId = calculationQueue.CalculationId,
				CalculationMethod = calculationQueue.CalculationMethod,
				CountbackId = calculationQueue.CountbackId,
				ElectionCategory = calculationQueue.ElectionCategory,
				ElectionElectorateVacancyId = calculationQueue.ElectionElectorateVacancyId,
				ExcludedCandidates = calculationQueue.ExcludedCandidates,
				InitialCalculationId = calculationQueue.InitialCalculationId,
				ResolvedTiedCandidate = calculationQueue.ResolvedTiedCandidate,
				Status = calculationQueue.Status,
				TieMethod = calculationQueue.TieMethod,
				TieResolutionLogic = calculationQueue.TieResolutionLogic,
				Vacancies = calculationQueue.Vacancies,
				VacatingCandidate = calculationQueue.VacatingCandidate,
			});
		}
	}

	public async Task Process(QueuedCalculation? queuedCalculation)
	{
		if (queuedCalculation != null)
		{
			if (queuedCalculation.CalculationMethod == (int)CalculationMethod.Countback)
			{
				await startNewCountbackTask(queuedCalculation);
			}
			else
			{
				await startNewCalculationTask(
					queuedCalculation.CalculationId,
					queuedCalculation.ElectionElectorateVacancyId,
					queuedCalculation.Vacancies,
					(CalculationMethod)queuedCalculation.CalculationMethod,
					(TieMethodType)queuedCalculation.TieMethod,
					(TieResolutionLogic)queuedCalculation.TieResolutionLogic,
					queuedCalculation.ExcludedCandidates,
					queuedCalculation.Status,
					queuedCalculation.ElectionCategory
					);
			}
		}
	}

	async Task startNewCountbackTask(QueuedCalculation queuedCalculation)
	{
		MCandidate vacatingCandidate = new MCandidate() { Position = queuedCalculation.VacatingCandidate.GetValueOrDefault() };

		CalculateWorkflow countbackPR = new CountbackPR(queuedCalculation.CalculationId
				, queuedCalculation.CountbackId.GetValueOrDefault()
				, queuedCalculation.ElectionElectorateVacancyId
				, queuedCalculation.Vacancies
				, queuedCalculation.InitialCalculationId.GetValueOrDefault()
				, vacatingCandidate
				, (TieMethodType)queuedCalculation.TieMethod
				, (TieResolutionLogic)queuedCalculation.TieResolutionLogic
				, queuedCalculation.ExcludedCandidates
				, (ElectionCategory)queuedCalculation.ElectionCategory
				, _dataContext
				, _reportingDal
				, _calculationContext
				, true);

		try
		{
			switch (queuedCalculation.Status.ToUpper())
			{
				case "QUED":
					countbackPR.Initialise();
					countbackPR.Calculate();
					break;
			}
		}
		catch (Exception ex)
		{
			try
			{				
				LogException(ex);
			}
			catch
			{
			}
		
			await _calculationContext.UpdateCalculationAsFailed(queuedCalculation.CalculationId, ex.Message);
		}
	}

	async Task startNewCalculationTask(Guid CalculationId, Guid ElectionElectorateVacancyId, int Vacancies, CalculationMethod CalculationMethod, TieMethodType TieMethodType, TieResolutionLogic TieResolutionLogic, string ExcludedCandidates, string Status, int ElectionCategory)
	{
		CalculateWorkflow wf;
		switch (CalculationMethod)
		{
			case CalculationMethod.Preferential:
				wf = new CalculatePref(CalculationId, ElectionElectorateVacancyId, TieMethodType, TieResolutionLogic, ExcludedCandidates, (ElectionCategory)ElectionCategory, _dataContext, _reportingDal, _calculationContext, IsSavingData: true);
				break;
			case CalculationMethod.Proportional:
				wf = new CalculatePR(CalculationId, ElectionElectorateVacancyId, Vacancies, TieMethodType, TieResolutionLogic, ExcludedCandidates, (ElectionCategory)ElectionCategory, _dataContext, _reportingDal, _calculationContext, IsSavingData: true);
				break;
			default:
				throw new NotImplementedException("Calculation Method Not Implemented.");
		}
		try
		{
			switch (Status.ToUpper())
			{
				case "QUED":
					wf.Initialise();
					wf.Calculate();
					break;
			}
		}
		catch (Exception ex)
		{
			LogException(ex);

			await _calculationContext.UpdateCalculationAsFailed(CalculationId, ex.Message);

			throw;
		}
	}

	void LogException(Exception x)
	{
		if (x is AggregateException)
		{
			logException(_logger, x);

			foreach (var exception in ((AggregateException)x).Flatten().InnerExceptions)
			{
				logException(_logger, exception);
			}
		}
		else
		{
			logException(_logger, x);
		}
	}

	static void logException(ILogger log, Exception x)
	{
		while (x != null)
		{
			log.LogError(x, x.Message);

			x = x.InnerException;
		}
	}
}
