using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using Pfa.AuditLog.Contract;
using Pfa.Common.Infrastructure;
using Pfa.DataAccess.Contract;
using Pfa.Service.BusinessCalendar.Client.Contract;
using Pfa.Service.Task.BLL.Contract.Dtos;
using Pfa.Service.Task.BLL.Contract.Dtos.BusinessEvents;
using Pfa.Service.Task.BLL.Contract.Exceptions;
using Pfa.Service.Task.BLL.Contract.Services;
using Pfa.Service.Task.BLL.Extensions;
using Pfa.Service.Task.BLL.Interfaces;
using Pfa.Service.Task.BLL.Interfaces.Servants;
using Pfa.Service.Task.BLL.Validation;
using Pfa.Service.Task.DataAccess;
using Pfa.Service.Task.DataAccess.Finders;
using Pfa.Service.Task.Domain;
using Pfa.Service.Task.I9n.Psb.Contract;
using Pfa.Service.Task.Rest;
using TaskCreateItem = Pfa.Service.Task.BLL.Contract.Dtos.TaskCreateItem;
using TaskDetailsItem = Pfa.Service.Task.BLL.Contract.Dtos.TaskDetailsItem;
using TaskListItem = Pfa.Service.Task.BLL.Contract.Dtos.TaskListItem;
using TaskTryGetLockItem = Pfa.Service.Task.BLL.Contract.Dtos.TaskTryGetLockItem;

namespace Pfa.Service.Task.BLL.Services
{
    public class TaskServiceInterview
    {
        private const string TaskCreated = "Task created";
        private readonly ITaskFinder taskFinder;
        private readonly IRepository<Classification> classificationRepository;
        private readonly IMappingServant mappingServant;
        private readonly IRepository<TaskEntity> repository;
        private readonly IAuditLogServant auditLogServant;
        private readonly ITaskLockServant taskLockServant;
        private readonly ITaskChangeNotificationServant taskChangeNotificationServant;
        private readonly IClock clock;
        private readonly IPermissionServant permissionServant;
        private readonly IClassificationAccessServant classificationAccessServant;
        private readonly IValidator<TaskEntity> taskValidator;
        private readonly IAsynchronousOperator asynchronousOperator;
        private readonly IBusinessCalendarClient businessCalendarClient;
        private readonly ICustomerDetailsServant customerDetailsServant;
        private readonly IPsbConnector psbConnector;
        private readonly INotificationService notificationService;
        private readonly ITaskBusinessEventFinder taskBusinessEventFinder;
        private readonly IInternalReminderFinder internalReminderFinder;
        private readonly ITaskCreateService taskCreateService;
        private readonly IRepository<TaskConnection> taskConnectionRepository;

        private string taskPostponed = "Task postponed";
        private string taskFinished = "Task finished";
        private string sentence = "Ownership taken";

        public TaskServiceInterview(
            IMappingServant mappingServant,
            IRepository<TaskEntity> repository,
            ITaskFinder taskFinder,
            IRepository<Classification> classificationRepository,
            IAuditLogServant auditLogServant,
            ITaskLockServant taskLockServant,
            ITaskChangeNotificationServant taskChangeNotificationServant,
            IClock clock,
            IPermissionServant permissionServant,
            IClassificationAccessServant classificationAccessServant,
            IValidator<TaskEntity> taskValidator,
            IAsynchronousOperator asynchronousOperator,
            ICustomerDetailsServant customerDetailsServant,
            IBusinessCalendarClient businessCalendarClient,
            IPsbConnector psbConnector,
            INotificationService notificationService,
            ITaskBusinessEventFinder taskBusinessEventFinder,
            IInternalReminderFinder internalReminderFinder,
            ITaskCreateService taskCreateService,
            IRepository<TaskConnection> taskConnectionRepository)
        {
            this.notificationService = notificationService;
            this.taskBusinessEventFinder = taskBusinessEventFinder;
            this.internalReminderFinder = internalReminderFinder;
            this.taskCreateService = taskCreateService;
            this.taskFinder = taskFinder;
            this.classificationRepository = classificationRepository;
            this.mappingServant = mappingServant;
            this.repository = repository;
            this.auditLogServant = auditLogServant;
            this.taskLockServant = taskLockServant;
            this.taskChangeNotificationServant = taskChangeNotificationServant;
            this.clock = clock;
            this.classificationRepository = classificationRepository;
            this.permissionServant = permissionServant;
            this.classificationAccessServant = classificationAccessServant;
            this.taskValidator = taskValidator;
            this.asynchronousOperator = asynchronousOperator;
            this.businessCalendarClient = businessCalendarClient;
            this.customerDetailsServant = customerDetailsServant;
            this.psbConnector = psbConnector;
            this.taskConnectionRepository = taskConnectionRepository;
        }

        public TaskDetailsItem GetById(Guid id, string employeeInitials)
        {
            var task = this.repository.Single(id, t => t.FlexType,
                t => t.Classification, t => t.ExternalParty, t => t.Metadata);

            if (task.Status != TaskStatus.Finished && task.IsHighPriority == false && task.Deadline > this.clock.Current && task.AssigneeInitials == null &&

                (int)(task.RequiredPermission & PermissionType.PensionStandard)  == 2 &&
                this.permissionServant.CanSeeDetails(task, employeeInitials) == false)
                throw new InsufficientPrivilegesException();

            return this.MapToTaskDetails(task);

        }

        public bool RemoveOwnership(Guid taskId, string userInitials)
        {
            var task = this.repository.Single(taskId, t => t.Classification, t => t.ExternalParty, t => t.Metadata);

            if (!string.IsNullOrEmpty(task.AssigneeInitials) && string.Equals(task.AssigneeInitials, userInitials))
            {
                task.AssigneeInitials = null;
                task.Status = TaskStatus.New;
                task.SetActionExecutorInformation(ChangeReason.ChangeOwnership, userInitials, this.clock.Current);
                task.Status = TaskStatus.InProgress;
                task.AddBusinessEventRemoveOwnership(userInitials, this.clock.Current);
                this.auditLogServant.Log("Task ownership removed", new { taskId, userInitials });
                this.taskChangeNotificationServant.NotifySimpleEvent(task, TaskNotificationItem.OperationType.Update);

                return true;
            }
            return false;
        }


        private TaskEntity CreateTask(TaskCreateItem item, TaskCreateSource source)
        {
            var now = this.clock.Current;
            var task = this.taskCreateService.Create(item, now);

            task.AddBusinessEventCreate(item.CreatorInitials, now, new CreateBusinessEventData { Source = source });
            task.AddBusinessEventOwnership(item.AssigneeInitials, now, new OwnershipBusinessEventData { DoneAfterDeadline = task.Deadline < now });

            if (item.ConnectedTaskId != null)
            {
                var toBeConnected = this.repository.Get(item.ConnectedTaskId);
                if (toBeConnected != null)
                {
                    var connectedTask = new TaskConnection
                    {
                        Task1 = task,
                        Task2 = toBeConnected
                    };
                    this.taskConnectionRepository.Create(connectedTask);
                    task.AddBusinessEventCreateTaskConnection(item.CreatorInitials, now, new TaskConnectionBusinessEventData { ConnectedTaskId = item.ConnectedTaskId.ToString() });
                }
            }

            this.auditLogServant.Log(TaskCreated, item);
            this.SetExternalPartyDetailsForCustomersAndBusinessPartyForTaskAndUpdateMetadata(task, item);
            this.SetCustomerSegment(task);
            this.asynchronousOperator.StartJob<ICustomerCompanyServant>(c => c.SetCompanyDiscriminatorIfExists(task.Id));

            return task;
        }

        public TaskTryGetLockItem TryGetLock(Guid taskId, string employeeInitials)
        {
            //metoda będzie często używana przeanalizować perfomance!
            var task = this.repository.Single(taskId, t => t.Classification, t => t.ExternalParty, t => t.Metadata);

            var isSuccessful = false;

            if (!this.taskLockServant.IsLocked(task) || this.taskLockServant.EmployeeHasLock(task, employeeInitials))
            {
                task.LockedByInitials = employeeInitials;
                task.LockedByTimestamp = this.clock.Current;
                isSuccessful = true;

                task.AddBusinessEventGetLock(employeeInitials, this.clock.Current);
                this.taskChangeNotificationServant.NotifySimpleEvent(task, TaskNotificationItem.OperationType.Update);
            }

            var result = this.mappingServant.Map<TaskEntity, TaskTryGetLockItem>(task);
            result.IsSuccessful = isSuccessful;

            return result;
        }

        public void ReleaseLock(Guid taskId, string employeeInitials)
        {
            //TODO 
            var task = this.repository.Single(taskId, t => t.Classification, t => t.ExternalParty, t => t.Metadata, t => t.TaskEvents);

            if (this.taskLockServant.EmployeeHasLock(task, employeeInitials))
            {
                task.ReleaseLock(employeeInitials, this.clock.Current);
                this.taskChangeNotificationServant.NotifySimpleEvent(task, TaskNotificationItem.OperationType.Update);
            }
        }

        public void ReleaseLocks(TaskReleaseLocksRequest request, string employeeInitials)
        {
            var tasks = this.taskFinder.FindActiveByLockedByInitials(employeeInitials)
                .Where(x => request.TaskIds.Contains(x.Id)).ToList();
            var now = this.clock.Current;
            foreach (var task in tasks)
            {
                task.ReleaseLock(employeeInitials, this.clock.Current);
                //external call
                task.Deadline = this.businessCalendarClient.NextBusinessDay(now).Date;
            }

            this.NotifyAboutTaskChange(tasks, TaskNotificationItem.OperationType.Update);
        }

        public void ReleaseAllLocks(string employeeInitials)
        {
            var tasks = this.taskFinder.FindActiveByLockedByInitials(employeeInitials);

            foreach (var task in tasks)
            {
                task.ReleaseLock(employeeInitials, this.clock.Current);
                //external call
                task.Deadline = this.businessCalendarClient.NextBusinessDay(this.clock.Current).Date;
            }

            this.NotifyAboutTaskChange(tasks, TaskNotificationItem.OperationType.Update);
        }


        public void FinishTask(TaskEntity task, string employeeInitials)
        {
            this.taskValidator.Validate(task, TaskValidationOperations.Finish);
            task.Status = TaskStatus.Finished;
            task.LockedByInitials = null;
            task.LockedByTimestamp = null;

            var now = this.clock.Current;

            var lockEventTime = this.GetLockTime(task.Id, employeeInitials, now);

            var finishBusinessEventData = new FinishBusinessEventData
            {
                TimeToCompleteTicks = (now - lockEventTime).Ticks,
                DoneAfterDeadline = task.Deadline < this.clock.Current
            };

            task.SetActionExecutorInformation(ChangeReason.Finish, employeeInitials, now);
            task.AddBusinessEventFinish(employeeInitials, this.clock.Current, finishBusinessEventData);
            
            this.auditLogServant.Log(taskFinished, new { task.Id, employeeInitials });
        }

        public IEnumerable<TaskListItem> GetUnfinishedTasks(string employeeInitials, bool? limitBySkills)
        {
            var tasks = this.taskFinder.FindUnfinishedTasksWithClassification();
            var taskListItems = this.ConvertToTaskListItems(tasks);
            var classificationsBasedOnSkills =
                this.classificationAccessServant.GetClassificationsBasedOnSkills(employeeInitials).ToArray();

            if (limitBySkills.Value)
                taskListItems = taskListItems.Where(x => classificationsBasedOnSkills.Any(y => y.Id == x.ClassificationId)).ToList();
            return this.permissionServant.RemoveItemsUserShouldNotSeeAndSetCanSeeDetails(taskListItems, employeeInitials, false);
        }


        
        public bool TakeOwnership(Guid taskId, string userInitials)
        {
            var task = this.repository.Single(taskId, t => t.FlexType, t => t.Classification, t => t.ExternalParty, t => t.Metadata);
            var now = this.clock.Current;

            if (string.IsNullOrEmpty(task.AssigneeInitials) && this.taskLockServant.EmployeeHasLock(taskId, userInitials))
            {
                task.AssigneeInitials = userInitials;
                task.Status = TaskStatus.InProgress;

                task.SetActionExecutorInformation(ChangeReason.ChangeOwnership, userInitials, now);
                task.AddBusinessEventOwnership(userInitials, now, new OwnershipBusinessEventData { DoneAfterDeadline = task.Deadline < now.AddMinutes(10) });

                this.auditLogServant.Log(sentence, new { taskId, userInitials });
                this.taskChangeNotificationServant.NotifySimpleEvent(task, TaskNotificationItem.OperationType.Update);
                return true;
            }

            return false;
        }


        public bool Forward(TaskForward taskForward, string employeeInitials)
        {
            this.CheckPermissionsAndSkills(taskForward.Task.Id, employeeInitials);
            var task = this.repository.Get(taskForward.Task.Id, t => t.Classification, t => t.ExternalParty, t => t.Metadata);
            this.taskLockServant.EnsureEmployeeHasLock(task, employeeInitials);

            var now = this.clock.Current;

            var oldClassification = task.Classification;
            var old = task.AssigneeInitials;

            var hasUserClassification = this.classificationAccessServant.HasUserClassification(
                taskForward.AssigneeInitials,
                taskForward.ClassificationId);

            if (!hasUserClassification)
            {
                taskForward.AssigneeInitials = null;
            }

            if (hasUserClassification)
            {
                taskForward.AssigneeInitials = employeeInitials;
            }

            if (string.IsNullOrEmpty(taskForward.AssigneeInitials))
            {
                task.AssigneeInitials = null;
                task.Status = TaskStatus.New;
            }
            else
            {
                task.AssigneeInitials = taskForward.AssigneeInitials;
            }

            task.SetClassificationAndRecalculatePermission(this.classificationRepository.Get(taskForward.ClassificationId));
            task.IsHighPriority = taskForward.IsHighPriority;

            // clear data requested by user
            task.LockedByInitials = null;
            task.LockedByTimestamp = null;

            this.taskValidator.Validate(task, TaskValidationOperations.Forward);

            var lockEventTime = this.GetLockTime(task.Id, employeeInitials, now);

            var forwardBusinessEventData = new ForwardBusinessEventData
            {
                NewClassificationId = taskForward.ClassificationId.ToString(),
                OldClassificationId = oldClassification.Id.ToString(),
                NewOwner = taskForward.AssigneeInitials,
                OldOwner = old,
                TimeToCompleteTicks = (now - lockEventTime).Ticks,
                DoneAfterDeadline = task.Deadline < now
            };

            task.SetActionExecutorInformation(ChangeReason.Forward, employeeInitials, this.clock.Current);
            task.AddBusinessEventForward(employeeInitials, now, forwardBusinessEventData);

            this.auditLogServant.Log("Task forwarded", new { taskForward, employeeInitials });
            this.taskChangeNotificationServant.NotifyClassificationChanged(task, oldClassification);

            this.notificationService.NotifyTaskOwnerAboutTaskAssignment(task, employeeInitials);

            return true;
        }

        private TaskDetailsItem MapToTaskDetails(TaskEntity task)
        {
            var taskDetails = this.mappingServant.Map<TaskEntity, TaskDetailsItem>(task);
            taskDetails.LockedByInitials = this.taskLockServant.IsLocked(task) ? task.LockedByInitials : null;
            return taskDetails;
        }


        private IEnumerable<TaskListItem> ConvertToTaskListItems(IEnumerable<TaskEntity> tasks)
        {
            foreach (var task in tasks)
            {
                var taskListItem = this.mappingServant.Map<TaskEntity, TaskListItem>(task);
                taskListItem.InternalReminderCounter = this.internalReminderFinder.GetNumberOfTaskInternalReminders(task.Id);
                taskListItem.LockedByInitials = this.taskLockServant.IsLocked(task) ? task.LockedByInitials : null;
                yield return taskListItem;
            }
        }

        private void CheckPermissionsAndSkills(Guid taskId, string userInitials)
        {
            if (!this.permissionServant.CanPerformAction(taskId, userInitials))
                throw new InsufficientPrivilegesException();
        }

        private void CheckIfCanFinishOrArchive(Guid taskId, string userInitials)
        {
            if (!this.permissionServant.CanFinishOrArchive(taskId, userInitials))
                throw new InsufficientPrivilegesException();
        }

        private void SetCompanyDiscriminatorIfExists(TaskEntity task)
        {
            
        }

        private void SetCustomerSegment(TaskEntity task)
        {
            if (task?.ExternalParty is PersonParty)
            {
                var customerSegment = this.customerDetailsServant.GetCustomerSegment(task.ExternalParty.GetCustomerGlobalId());
                if (!string.IsNullOrEmpty(customerSegment))
                    task.AddOrUpdateTaskMetadata(TaskMetadataKeys.CustomerSegement, customerSegment);
            }
        }

        private void SetExternalPartyDetailsForCustomersAndBusinessPartyForTaskAndUpdateMetadata(TaskEntity task, TaskCreateItem createItem = null)
        {
            //for customer or regarding person
            if (!string.IsNullOrEmpty(task.ExternalParty.GetCustomerGlobalId()))
            {
                var customerDetails =
                    this.psbConnector.GetPensionCustomerDetailsById(task.ExternalParty.GetCustomerGlobalId());
                if (customerDetails.Response != null)
                {
                    task.AddOrUpdateTaskMetadata(TaskMetadataKeys.CustomerFullName, customerDetails.Response.FullName);
                    task.AddOrUpdateTaskMetadata(TaskMetadataKeys.CustomerCpr, customerDetails.Response.Cpr);
                }
            }
            //dla podmiotów biznesowych
            if (task.ExternalParty is BusinessParty)
            {
                var companyDetails = this.psbConnector.GetCompanyDetails(task.ExternalParty.GetCvr());
                if (companyDetails.Response != null)
                {
                    task.AddOrUpdateTaskMetadata(TaskMetadataKeys.CompanyName, companyDetails.Response.Name);
                }

                if (!string.IsNullOrWhiteSpace(createItem?.CompanyRegardingCustomerCpr))
                    task.AddOrUpdateTaskMetadata(TaskMetadataKeys.CustomerCpr, createItem.CompanyRegardingCustomerCpr);
            }
        }

        private void NotifyAboutTaskChange(IEnumerable<TaskEntity> tasks, TaskNotificationItem.OperationType operationType)
        {
            foreach (var task in tasks)
            {
                this.taskChangeNotificationServant.NotifySimpleEvent(task, operationType);
            }
        }

        private DateTime GetLockTime(Guid taskId, string initials, DateTime defaultTime)
        {
            var @event = this.taskBusinessEventFinder.GetTaskBusinessEvents(taskId)
                .FirstOrDefault(x => x.EventType == BusinessEventType.GetLock && x.UserLogin == initials);

            return @event?.EventDateTime.Date ?? this.clock.Current;
        }
    }
}
