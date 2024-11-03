using System;
using System.Collections.Generic;
using System.Linq;
using Vec.Common.Entities;
using Vec.ComputerCount.DAL;
using Vec.ComputerCount.DALReporting;
using Vec.ComputerCount.Entities;
using Vec.ComputerCount.BL;
using Vec.CalculationEngine.DAL;

namespace Vec.Calculation.BL.Workflow
{
    public enum ProcessStates
    {
        CheckCalculateState = 1,
        ExcludeCandidate,
        ExclusionTie,
        Finished,
        Get1stTiedSurplusGroup,
        Stop,
        ElectWhileVacancyExists
    }

    public class CalculateWorkflow: CalculateBL
    {
        protected Guid ProcessId;
        protected MCalculateResponse Response = new MCalculateResponse();
        protected ICalculationContext ICalculationContext;
        protected List<MCandidate> OverQuotaCandidates;
        protected List<MCandidate> Excluded;
        protected List<MCandidateEx> CurrentSurplusTiedCandidate;
        protected List<MCandidateEx> TiedGroupSegment;
        protected List<MCandidate> TiedCandidate;
        protected ElectionCategory ElectionCategory;

        protected ProcessStates ProcessState;
        public bool IsSavingData { get; set; }

        public CalculateWorkflow(Guid CalculationId, Guid ElectionElectorateVacancyId, int Vacancies, CalculationMethod CalcMethod, TieMethodType TieMethod, TieResolutionLogic TieResolutionLogic, string ExcludedCandidates, ElectionCategory ElectionCategory,
						IComputerCountDataContext dataContext, IReportingDAL reporting, ICalculationContext calculationContext, bool IsSavingData) :
            base(ElectionElectorateVacancyId, Vacancies, CalcMethod, TieMethod, TieResolutionLogic, ExcludedCandidates, dataContext, reporting)
        {
            this.ICalculationContext = calculationContext;
            this.CalculationId = this.ProcessId = CalculationId;
            this.IsSavingData = IsSavingData;
            this.TieResolutionLogic = TieResolutionLogic;
            this.ElectionCategory = ElectionCategory;
        }

        protected void SaveData()
        {
            if (IsSavingData)
            {
                base.Save();
            }
        }

        protected void UpdateQueueAsSuccessful()
        {
            if (IsSavingData)
            {
                ICalculationContext.UpdateCalculationAsSuccessful(CalculationId);
            }
        }
        public virtual void Initialise()
        {
            try
            {
                ICalculationContext.UpdateCalculationAsLoading(CalculationId);

                this.LoadPapers();
            }
            catch
            {
                this.CalculationStatus = CalculateState.Error;
                throw;
            }
        }

        public virtual void Calculate()
        {
            ICalculationContext.UpdateCalculationAsWorking(CalculationId);

            this.QueueDistributionOf1stPreferences();
            ResumeCalculation(CalculateState.Starting);
        }

        protected virtual void ResumeCalculation(CalculateState state)
        {
        }

        protected List<MCandidateEx> Get1stTiedSurplusGroup(List<MCandidateEx> all)
        {
            var result = new List<MCandidateEx>();

            var unresolved = (from a in all
                              where !a.IsTieResolved && a.IsTie
                              select a).ToList();

            if (unresolved.Count() > 0)
            {
                int max = unresolved.Max(x => x.CurrentTotalVotes);

                result.AddRange(from u in unresolved
                                where u.CurrentTotalVotes == max
                                select u);
            }

            return result;
        }

        protected List<MCandidateEx> CandidatesResolvedOrderActivity(List<MCandidateEx> mainList, List<MCandidate> resolved)
        {
            List<int> indexlist = new List<int>();

            foreach (MCandidate r in resolved)
            {
                MCandidateEx m = mainList.Where(x => x.Position == r.Position).FirstOrDefault();
                if (m != null)
                {
                    indexlist.Add(mainList.IndexOf(m));
                }
            }

            List<int> targetIndex = indexlist.OrderBy(x => x).ToList();

            var result = new List<MCandidateEx>(mainList);

            for (int index = 0; index < indexlist.Count; index++)
            {
                MCandidateEx m = mainList.Where(x => x.Position == resolved[index].Position).FirstOrDefault();
                m.IsTieResolved = true;
                result[targetIndex[index]] = m;
            }

            return result;
        }
        protected List<MCandidate> CandidateEx2Candidate(List<MCandidateEx> input)
        {
            return (from i in input
                    select new MCandidate
                    {
                        Position = i.Position,
                        Name = i.Name
                    }).ToList();
        }

        protected List<MCandidateEx> Candidate2CandidateEx(List<MCandidate> input)
        {
            return (from x in input
                    select new MCandidateEx
                    {
                        Position = x.Position,
                        Name = x.Name,
                        IsTie = true,
                        IsTieResolved = false,
                    }).ToList();
        }

        protected List<MCandidate> RefreshOverQuotaCandidates(List<MCandidate> mainList, List<MCandidateEx> resolved)
        {
            List<int> indexlist = new List<int>();

            foreach (MCandidate r in resolved)
            {
                MCandidate m = mainList.Where(x => x.Position == r.Position).FirstOrDefault();
                if (m != null)
                {
                    indexlist.Add(mainList.IndexOf(m));
                }
            }

            List<int> targetIndex = indexlist.OrderBy(x => x).ToList();

            var result = new List<MCandidate>(mainList);

            for (int index = 0; index < indexlist.Count; index++)
            {
                MCandidate m = mainList.Where(x => x.Position == resolved[index].Position).FirstOrDefault();
                result[targetIndex[index]] = m;
            }

            return result;
        }

        public void ResolveTie(MResolveRequest request)
        {
            if (request.SelectedCandidate != null)
            {
                switch (this.CalculationStatus)
                {
                    case CalculateState.ExclusionTie:
                        ExcludeCandidate(request.SelectedCandidate);
                        ResumeCalculation(CalculateState.ExclusionTieResolved);
                        break;
                    case CalculateState.ElectionTie:
                        this.CurrentSurplusTiedCandidate = this.CandidatesResolvedOrderActivity(this.CurrentSurplusTiedCandidate, request.PRResolvedCandidates);
                        ResumeCalculation(CalculateState.ElectionTieResolved);
                        break;
                }
            }
            else
            {
                throw new Exception("No Candidate has been selected for the tie.");
            }
        }
    }

    public class CalculatePref : CalculateWorkflow
    {
        public CalculatePref(Guid CalculationId, Guid ElectionElectorateVacancyId, TieMethodType TieMethod, TieResolutionLogic TieResolutonLogic, string ExcludedCandidates, ElectionCategory ElectionCategory,
            IComputerCountDataContext dataContext, IReportingDAL reporting, ICalculationContext calculationContext, bool IsSavingData) :
            base(CalculationId, ElectionElectorateVacancyId, 1, CalculationMethod.Preferential, TieMethod, TieResolutonLogic, ExcludedCandidates, ElectionCategory,
                                dataContext, reporting, calculationContext, IsSavingData)
        {
        }

        public override void Calculate()
        {
            if (this.ElectionCategory == ElectionCategory.Municipal)
            {
                foreach (var p in this.InitialExcludedCandidates)
                {
                    AddExcludedCandidate(p);
                }
            }
            base.Calculate();
        }

        protected override void ResumeCalculation(CalculateState status)
        {
            this.CalculationStatus = status;

            this.ProcessState = ProcessStates.CheckCalculateState;

            while (this.ProcessState != ProcessStates.Stop)
            {
                switch (this.ProcessState)
                {
                    case ProcessStates.CheckCalculateState:
                       
                        if (ContinuingCandidates.Count != 1 || VacanciesToFill != 1)
                        {
                            this.SaveData();
                            this.RunNextQueuedDistribution();
                            this.OverQuotaCandidates = GetPassedQuotaCandidates();

                            if (OverQuotaCandidates.Count == 0)
                            {
                                this.Excluded = GetCandidatesWithLowestVotes();

                                if (this.Excluded.Count == 1)
                                {
                                    this.ProcessState = ProcessStates.ExcludeCandidate;
                                }
                             
                                else if (this.ElectionCategory == ElectionCategory.Parliamentary &&
                                    this.Excluded.Count == this.ContinuingCandidates.Count && this.ContinuingCandidates.Count == 2)
                                {
                                    this.ProcessState = ProcessStates.Finished;
                                }
                                else if (TieMethodType != Common.Entities.TieMethodType.Automatic)
                                {
                                    this.ProcessState = ProcessStates.ExclusionTie;
                                }
                                else
                                {
                                    if (VacanciesToFill != 1 || ContinuingCandidates.Count != 2)
                                    {
                                        this.Excluded = ResolvePRExclusionTie(this.Excluded);
                                        this.ProcessState = ProcessStates.ExcludeCandidate;
                                    }
                                    else
                                    {
                                        var index = Select0Or1Randomly();
                                        ElectCandidate(ContinuingCandidates[index]);
                                        this.Response.TiedCandidateRandomlyElected = true;
                                        this.ProcessState = ProcessStates.Finished;
                                        break;
                                    }
                                }
                            }
                            else
                            {
                                this.ElectCandidate(this.OverQuotaCandidates[0]);
                                this.ProcessState = ProcessStates.Finished;
                            }
                        }
                        else
                        {
                            this.ElectCandidate(this.ContinuingCandidates[0]);
                            this.ProcessState = ProcessStates.Finished;
                        }
                        break;
                    case ProcessStates.ExcludeCandidate:
                        ExcludeCandidate(this.Excluded[0]);
                        this.ProcessState = ProcessStates.CheckCalculateState;
                        break;

                    case ProcessStates.ExclusionTie:
                        this.TiedCandidate = this.Excluded;
                        this.CalculationStatus = CalculateState.ExclusionTie;
                        this.ProcessState = ProcessStates.Stop;
                        break;

                    case ProcessStates.Finished:
                        this.CalculationStatus = CalculateState.Finished;
                        this.SaveData();
                        this.UpdateQueueAsSuccessful();
                        this.ProcessState = ProcessStates.Stop;
                        break;
                }

            }

        }

    }

    public class CalculatePR : CalculateWorkflow
    {
        public CalculatePR(Guid CalculationId, Guid ElectionElectorateVacancyId, int Vacancies, TieMethodType TieMethod, TieResolutionLogic TieResolutionLogic, string ExcludedCandidates, ElectionCategory ElectionCategory,
            IComputerCountDataContext dataContext, IReportingDAL reporting, ICalculationContext calculationContext, bool IsSavingData) :
            base(CalculationId, ElectionElectorateVacancyId, Vacancies, CalculationMethod.Proportional, TieMethod, TieResolutionLogic, ExcludedCandidates, ElectionCategory,
                                dataContext, reporting, calculationContext, IsSavingData)
        {
            var InitialExcludedCandidates = string.IsNullOrWhiteSpace(ExcludedCandidates) ? new List<MCandidate>() :
                ExcludedCandidates.Split(',').Select(x => new MCandidate { Name = "", Position = int.Parse(x) }).ToList();
            if (InitialExcludedCandidates.Count > 0)
            {
                InitialExcludedCandidates.ForEach(p => base.AddExcludedCandidate(p));
            }
        }

        protected override void ResumeCalculation(CalculateState status)
        {
            this.CalculationStatus = status;

            this.ProcessState = ProcessStates.CheckCalculateState;

            while (this.ProcessState != ProcessStates.Stop)
            {
                switch (this.ProcessState)
                {
                    case ProcessStates.CheckCalculateState:
                       
                        if (ContinuingCandidates.Count != 1 || VacanciesToFill != 1)
                        {
                            this.SaveData();
                            this.RunNextQueuedDistribution();
                            this.OverQuotaCandidates = GetPassedQuotaCandidates();
                            if (OverQuotaCandidates.Count == 0)
                            {
                                if (!IsQueuedDistributions)
                                {
                                    if (VacanciesToFill != ContinuingCandidates.Count)
                                    {
                                        this.Excluded = GetCandidatesWithLowestVotes();
                                        if (this.Excluded.Count == 1)
                                        {
                                            this.ProcessState = ProcessStates.ExcludeCandidate;
                                        }
                                        else if (TieMethodType != Common.Entities.TieMethodType.Automatic)
                                        {
                                            this.ProcessState = ProcessStates.ExclusionTie;
                                        }
                                        else
                                        {
                                            if (VacanciesToFill != 1 || ContinuingCandidates.Count != 2)
                                            {
                                                this.Excluded = ResolvePRExclusionTie(this.Excluded);
                                                this.ProcessState = ProcessStates.ExcludeCandidate;
                                            }
                                            else
                                            {
                                                var index = Select0Or1Randomly();
                                                ElectCandidate(ContinuingCandidates[index]);
                                                this.Response.TiedCandidateRandomlyElected = true;
                                                this.ProcessState = ProcessStates.Finished;
                                                break;
                                            }
                                        }
                                    }
                                    else
                                    {
                                        this.CurrentSurplusTiedCandidate = this.Candidate2CandidateEx(this.ContinuingCandidates);
                                        this.OverQuotaCandidates = this.ResolvePRSurplusTie(this.CurrentSurplusTiedCandidate);
                                        this.ProcessState = ProcessStates.ElectWhileVacancyExists;
                                    }
                                }
                            }
                            else
                            {
                                this.CurrentSurplusTiedCandidate = this.GetSurplusTiedCandidates(this.OverQuotaCandidates);
                                this.ProcessState = ProcessStates.Get1stTiedSurplusGroup;
                            }
                        }
                        else
                        {
                            this.ElectCandidate(this.ContinuingCandidates[0]);
                            this.ProcessState = ProcessStates.Finished;
                        }
                        break;
                    case ProcessStates.ExcludeCandidate:
                        ExcludeCandidate(this.Excluded[0]);
                        this.ProcessState = ProcessStates.CheckCalculateState;
                        break;

                    case ProcessStates.ExclusionTie:
                        this.TiedCandidate = this.Excluded;
                        this.CalculationStatus = CalculateState.ExclusionTie;
                        this.ProcessState = ProcessStates.Stop;
                        break;

                    case ProcessStates.Get1stTiedSurplusGroup:
                        this.TiedGroupSegment = this.Get1stTiedSurplusGroup(this.CurrentSurplusTiedCandidate);

                        if (this.TiedGroupSegment.Count > 0)
                        {
                            if (this.TieMethodType == TieMethodType.Automatic)
                            {
                                var PRResolvedCandidates = this.ResolvePRSurplusTie(this.TiedGroupSegment);
                                this.CurrentSurplusTiedCandidate = this.CandidatesResolvedOrderActivity(this.CurrentSurplusTiedCandidate, PRResolvedCandidates);
                                this.ProcessState = ProcessStates.Get1stTiedSurplusGroup;
                            }
                            else
                            {
                                this.TiedCandidate = this.CandidateEx2Candidate(this.TiedGroupSegment);
                                this.CalculationStatus = CalculateState.ElectionTie;
                                this.ProcessState = ProcessStates.Stop;
                            }
                        }
                        else
                        {
                            this.OverQuotaCandidates = this.RefreshOverQuotaCandidates(this.OverQuotaCandidates, this.CurrentSurplusTiedCandidate);
                            this.CurrentSurplusTiedCandidate.Clear();
                            this.QueueSurplusDistributions(this.OverQuotaCandidates);

                            this.ProcessState = ProcessStates.ElectWhileVacancyExists;

                        }
                        break;

                    case ProcessStates.Finished:
                        this.CalculationStatus = CalculateState.Finished;
                        this.SaveData();
                        this.UpdateQueueAsSuccessful();
                        this.ProcessState = ProcessStates.Stop;
                        break;

                    case ProcessStates.ElectWhileVacancyExists:
                        var counter = 0;
                        do
                        {
                            ElectCandidate(this.OverQuotaCandidates[counter++]);
                        } while (!this.IsVacanciesFilled && counter < this.OverQuotaCandidates.Count);

                        if (this.IsVacanciesFilled)
                        {
                            this.ProcessState = ProcessStates.Finished;
                        }
                        else
                        {
                            this.CalculationStatus = CalculateState.Working;
                            this.ProcessState = ProcessStates.CheckCalculateState;
                        }
                        break;
                }

            }

        }
    }
}
