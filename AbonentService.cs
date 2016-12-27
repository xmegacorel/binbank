using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Nelibur.Sword.Core;
using Nelibur.Sword.DataStructures;
using Nelibur.Sword.Extensions;
using NodaTime;
using Serilog;
using SmartAirKey.Core;
using SmartAirKey.Core.Events;
using SmartAirKey.Core.Extensions;
using SmartAirKey.Core.Time;
using SmartAirKey.KeyServer.Core;
using SmartAirKey.KeyServer.Core.Validation;
using SmartAirKey.KeyServer.Intercoms.Abonents.Events;
using SmartAirKey.KeyServer.Intercoms.ServiceCompanies;
using SmartAirKey.KeyServer.Intercoms.Tariffs;
using SmartAirKey.KeyServer.SharedKernel;
using SmartAirKey.KeyServer.Territory.AccessObjects;
using SmartAirKey.KeyServer.Territory.AccessPerimeters;
using SmartAirKey.KeyServer.Users;

namespace SmartAirKey.KeyServer.Intercoms.Abonents
{
    public class AbonentService : IAbonentService
    {
        // тест на данный класс находится в ChangeTerritoryConfigurationTests

        private readonly AbonentsRepository _abonentsRepository;
        private readonly AccessPerimeterRepository _accessPerimeterRepository;
        private readonly ITariffPolicyService _tariffPolicyService;
        private readonly IAccessObjectService _accessObjectService;
        private readonly IUserPublicService _userPublicService;

        public AbonentService(
            AbonentsRepository abonentsRepository,
            IUserPublicService userPublicService,
            AccessPerimeterRepository accessPerimeterRepository,
            ITariffPolicyService tariffPolicyService,
            IAccessObjectService accessObjectService)
        {
            _abonentsRepository = abonentsRepository;
            _userPublicService = userPublicService;
            _accessPerimeterRepository = accessPerimeterRepository;
            _tariffPolicyService = tariffPolicyService;
            _accessObjectService = accessObjectService;
        }

        public async Task<List<Abonent>> GetAbonentsByServiceCompanyId(Identity<ServiceCompany> companyId)
        {
            return await _abonentsRepository.GetAllByServiceCompanyAsync(companyId);
        }

        /// <summary>
        /// Добавление абонента из админки Сервисной компании.
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        public async Task<Result<Abonent>> RegisterAsync(RegisterAbonentRequest request)
        {
            Check.NotNull(request, nameof(request));

            var existingAbonent = await _abonentsRepository.GetByPhoneNumberAsync(request.PhoneNumber, request.ServiceCompanyId);
            if (existingAbonent.HasValue)
            {
                return new AbonentAlreadyRegisteredErrorReason(request.PhoneNumber);
            }

            Result validPerimetersAndTariffs = await VerifyPerimetersAndTariffsAsync(request.Perimeters, request.ServiceCompanyId, request.TemporaryAccessPerimeters);
            if (validPerimetersAndTariffs.Failure)
            {
                return validPerimetersAndTariffs.Error;
            }

            var abonent = new Abonent
            {
                Id = GuidComb.New(),
                DisplayName = request.DisplayName,
                PhoneNumber = request.PhoneNumber,
                Comments = request.Comments,
                AddressId = request.AddressId,
                Room = request.Room,
                ServiceCompanyId = request.ServiceCompanyId,
                Cars = request.Cars,
                ExternalId = request.ExternalId,
                IsAdministrator = request.IsAdministrator,
                WorkingTime = request.WorkingTime,
                TemporaryAccessPerimeters = request.TemporaryAccessPerimeters
            };
            abonent.ApplyPerimeters(request.Perimeters);

            var existingUser = await _userPublicService.QueryAsync(request.PhoneNumber);
            if (existingUser.Success)
            {
                abonent.UserId = existingUser.Value.Id;
            }

            await _abonentsRepository.AddAsync(abonent);

            var payload = await GetAccessObjectPayloadAsync(abonent);

            payload = payload.Union(abonent.GetPayload()).ToList();

            var resultFamily = await EventHelper.RaiseEventAsync(FamilyKeyPerimetersAdded, new FamilyKeyPerimetersAddedEventArgs()
            {
                Perimeters = abonent.Perimeters,
                User = abonent.GetUser(),
                ServiceCompanyId = abonent.ServiceCompanyId,
                Payload = payload
            });
            var resultTemporary = await EventHelper.RaiseEventAsync(TemporaryKeyPerimetersAdded, new TemporaryKeyPerimetersAddedEventArgs()
            {
                ServiceCompanyId = abonent.ServiceCompanyId,
                User = abonent.GetUser(),
                Payload = payload,
                Perimeters = abonent.TemporaryAccessPerimeters
            });

            return Result.Combine(resultFamily, resultTemporary).OnSuccess(() => abonent);
        }

        public async Task<Result> UnregisterAsync(UnregisterAbonentRequest request)
        {
            Check.NotNull(request, nameof(request));

            var existingAbonent = await Validator.Validate<UnregisterAbonentRequest, UnregisterAbonentRequest.Validator>(request)
                .OnSuccessAsync(x => _abonentsRepository.GetByIdAsync(request.AbonentId, request.ServiceCompanyId), () => new AbonentNotFoundErrorReason());

            return await existingAbonent
                .OnSuccessAsync(abonent => _abonentsRepository.DeleteAsync(abonent.ToIdentity()))
                .OnSuccessAsync(abonent => EventHelper.RaiseEventAsync(PerimetersRemoved, new AbonentPerimetersRemovedEventArgs()
                {
                    ServiceCompanyId = abonent.ServiceCompanyId,
                    RemovedPerimeterIds = abonent.Perimeters.Select(t => t.AccessPerimeterId).Distinct().ToList(),
                    User = abonent.GetUser()
                }));
        }

        public async Task<Result<Abonent>> UpdateAsync(UpdateAbonentRequest request)
        {
            Check.NotNull(request, nameof(request));

            Result validPerimetersAndTariffs = await VerifyPerimetersAndTariffsAsync(request.Perimeters, request.ServiceCompanyId, request.TemporaryAccessPerimeters);
            if (validPerimetersAndTariffs.Failure)
            {
                return validPerimetersAndTariffs.Error;
            }

            var existingAbonent = await _abonentsRepository.GetByIdAsync(request.AbonentId, request.ServiceCompanyId)
                .ToResultAsync(() => new AbonentNotFoundErrorReason());

            if (existingAbonent.Failure)
                return existingAbonent;

            var abonent = existingAbonent.Value;

            abonent.DisplayName = request.DisplayName;
            abonent.Comments = request.Comments;

            // TODO #KS-196 Change abonent phone number
            // userid may change too
            //abonent.PhoneNumber = request.PhoneNumber;

            // changing address may affect reports
            abonent.Room = request.Room;
            abonent.AddressId = request.AddressId;

            var carChangeSet = abonent.GetCarChanges(request.Cars);
            abonent.Cars = request.Cars;

            var tmpPerimeters = abonent.GetTempararyPerimeterChanges(request.TemporaryAccessPerimeters);
            MergeChangedTemporaryPerimeters(tmpPerimeters, abonent);

            AbonentPerimeterChangeSet perimeterChangeSet = abonent.ApplyPerimeters(request.Perimeters);

            var changesAttribute = abonent.ChangesAttributeAbonent(request);

            await _abonentsRepository.UpdateAsync(abonent);

            await NofityCarPerimeterChangesAbonent(carChangeSet, abonent, perimeterChangeSet, tmpPerimeters);

            await NotifyChangesAttributeAbonent(changesAttribute, abonent);

            return abonent;
        }

        private void MergeChangedTemporaryPerimeters(Abonent.AbonentTempararyPerimeterChangeSet tmpPerimeters, Abonent abonent)
        {
            foreach (var perimeter in tmpPerimeters.Removed)
            {
                var item = abonent.TemporaryAccessPerimeters.SingleOrDefault(t => t.PerimeterId == perimeter.PerimeterId);
                if (item != null)
                {
                    item.Removed = true;
                }
            }

            tmpPerimeters.Added.ForEach(t => abonent.TemporaryAccessPerimeters.Add(t));

        }

        public async Task<Result> DeleteTemporaryPerimeterAbonentAsync(Identity<User> userId, Identity<ServiceCompany> companyId, Identity<AccessPerimeter> perimeterId)
        {
            Log.Debug("Start delete temporary perimeter from user: {UserId} {CompanyId} {PerimeterId}", userId, companyId, perimeterId);

            var abonent = await _abonentsRepository.GetByUserIdAsync(userId, companyId);
            var found = false;
            foreach (var item in abonent.TemporaryAccessPerimeters)
            {
                if (item.PerimeterId == perimeterId)
                {
                    found          = true;
                    item.Removed   = true;
                    item.RemovedAt = TimeServer.Now();
                    break;
                }
            }

            if (found)
            {
                await _abonentsRepository.UpdateAsync(abonent);
                Log.Debug("Temporary perimeter deleted from abonent: {Abonent} {PerimeterId}", abonent.DisplayName, perimeterId);
            }

            return Result.Ok();
        }

        private async Task NofityCarPerimeterChangesAbonent(
            Abonent.AbonentCarChangeSet carChanges, Abonent abonent, AbonentPerimeterChangeSet changeSet, Abonent.AbonentTempararyPerimeterChangeSet tmpPerimeters)
        {
            if (carChanges.IsEmpty() == false && abonent.UserId.HasValue)
            {
                await EventHelper.RaiseEventAsync(CarsChanged, new AbonentCarsChangedEventArgs()
                {
                    UserId = abonent.UserId.Value.ToIdentity<User>(),
                    Added = carChanges.AddedCars,
                    Deleted = carChanges.DeletedCars,
                    CompanyId = abonent.ServiceCompanyId,
                    PerimeterIds = abonent.Perimeters.Select(t => t.AccessPerimeterId).Union(abonent.TemporaryAccessPerimeters.Select(x => x.PerimeterId)),
                    PhoneNumber = abonent.PhoneNumber
                });
            }

            var payload = abonent.GetPayload();

            if (changeSet.AddedPerimeters.IsNotEmpty())
            {
                await EventHelper.RaiseEventAsync(FamilyKeyPerimetersAdded, new FamilyKeyPerimetersAddedEventArgs
                {
                    Perimeters = changeSet.AddedPerimeters,
                    User = abonent.GetUser(),
                    ServiceCompanyId = abonent.ServiceCompanyId,
                    Payload = payload
                });
            }

            if (tmpPerimeters.Added.IsNotEmpty())
            {
                await EventHelper.RaiseEventAsync(TemporaryKeyPerimetersAdded, new TemporaryKeyPerimetersAddedEventArgs
                {
                    Perimeters = tmpPerimeters.Added,
                    User = abonent.GetUser(),
                    ServiceCompanyId = abonent.ServiceCompanyId,
                    Payload = payload
                });
            }

            if (changeSet.RemovedPerimeterIds.IsNotEmpty() || tmpPerimeters.Removed.IsNotEmpty())
            {
                await EventHelper.RaiseEventAsync(PerimetersRemoved, new AbonentPerimetersRemovedEventArgs()
                {
                    ServiceCompanyId = abonent.ServiceCompanyId,
                    RemovedPerimeterIds = changeSet.RemovedPerimeterIds.Union(tmpPerimeters.Removed.Select(t => t.PerimeterId)).ToList(),
                    User = abonent.GetUser()
                });
            }

            if (changeSet.ChangedTariffPerimeters.IsNotEmpty())
            {
                await EventHelper.RaiseEventAsync(PerimetersTariffChanged, new AbonentPerimetersTariffChangedEventArgs()
                {
                    ServiceCompanyId = abonent.ServiceCompanyId,
                    Perimeters = changeSet.ChangedTariffPerimeters,
                    User = abonent.GetUser()
                });
            }
        }

        private async Task NotifyChangesAttributeAbonent(List<RequestPayloadBase> payload, Abonent abonent)
        {
            if (payload.Count > 0)
                await EventHelper.RaiseEventAsync(AbonentAttributeChanged, new AbonentAttributeChangedEventArgs()
                {
                    UserId = abonent.UserId.Value.ToIdentity<User>(),
                    CompanyId = abonent.ServiceCompanyId,
                    Payload = payload,
                    PerimeterIds = abonent.Perimeters.Select(t => t.AccessPerimeterId).Union(abonent.TemporaryAccessPerimeters.Select(x => x.PerimeterId)),
                    PhoneNumber = abonent.PhoneNumber
                });
        }

        public async Task<Result> DeleteFamilyPerimeterAbonentAsync(Identity<User> userId, Identity<ServiceCompany> companyId, Identity<AccessPerimeter> perimeterId)
        {
            Log.Debug("Start delete family perimeter from user: {UserId} {CompanyId} {PerimeterId}", userId, companyId, perimeterId);

            var abonent = await _abonentsRepository.GetByUserIdAsync(userId, companyId);
            var found = false;
            for (int i = 0; i < abonent.Perimeters.Count; i++)
            {
                var item = abonent.Perimeters[i];
                if (item.AccessPerimeterId == perimeterId)
                {
                    found = true;
                    abonent.Perimeters.Remove(item);
                    break;
                }
            }

            if (found)
            {
                await _abonentsRepository.UpdateAsync(abonent);
                Log.Debug("Family perimeter deleted from abonent: {Abonent} {PerimeterId}", abonent.DisplayName, perimeterId);
            }

            return Result.Ok();
        }

        private async Task<Result> VerifyPerimetersAndTariffsAsync(List<AbonentPerimeter> abonentPerimeters, Identity<ServiceCompany> companyId, 
                                    List<TemporaryAccessPerimeter> temporaryAccessPerimeters)
        {
            var perimeterIds = abonentPerimeters.ConvertAll(x => x.AccessPerimeterId);
            if (perimeterIds.IsNotEmpty())
            {
                if (perimeterIds.Count != perimeterIds.Distinct().Count())
                {
                    return new OperationCanceledErrorReason().Add("Ensure all perimeters are different (duplicate perimeters found)");
                }

                var foundPerimeters = await _accessPerimeterRepository.GetAllAsync(perimeterIds, companyId);
                if (foundPerimeters.Count != perimeterIds.Count)
                {
                    return new OperationCanceledErrorReason().Add("Ensure all perimeters are registered");
                }
            }

            var tariffPolicyIds = abonentPerimeters.ConvertAll(x => x.TariffPolicyId);
            if (tariffPolicyIds.IsNotEmpty())
            {
                var distinctTariffPolicyIdsCount = tariffPolicyIds.Distinct().Count();
                var foundTariffPolicies = await _tariffPolicyService.GetAllAsync(tariffPolicyIds);
                var foundCustomerTariffPoliciesCount = foundTariffPolicies.Count(x => x.ServiceCompanyId == companyId);
                if (foundCustomerTariffPoliciesCount != distinctTariffPolicyIdsCount)
                {
                    return new OperationCanceledErrorReason().Add("Ensure all tariff policies are registered");
                }
            }

            var tmpPerimeterIds = temporaryAccessPerimeters.ConvertAll(x => x.PerimeterId);
            if (tmpPerimeterIds.IsNotEmpty())
            {
                if (tmpPerimeterIds.Count != tmpPerimeterIds.Distinct().Count())
                {
                    return new OperationCanceledErrorReason().Add("Ensure all temporary perimeters are different (duplicate perimeters found)");
                }

                var foundPerimeters = await _accessPerimeterRepository.GetAllAsync(tmpPerimeterIds, companyId);
                if (foundPerimeters.Count != tmpPerimeterIds.Count)
                {
                    return new OperationCanceledErrorReason().Add("Ensure all temporary perimeters are registered");
                }
            }

            return Result.Ok();
        }

        private async Task<ICollection<RequestPayloadBase>> GetAccessObjectPayloadAsync(Abonent abonent)
        {
            var perimeterIds = abonent.Perimeters.Select(t => t.AccessPerimeterId).Union(abonent.TemporaryAccessPerimeters.Select(t => t.PerimeterId));
            var fullPerimeters = await _accessPerimeterRepository.FilterAsync(perimeterIds);

            var accessPerimeter = fullPerimeters.First();
            var fullObject = await _accessObjectService.GetByIdAsync(accessPerimeter.AccessObjectId, accessPerimeter.ServiceCompanyId);

            var collection = new List<RequestPayloadBase>() { new RequestAccessObjectDisplayNamePayload() { DisplayName = fullObject.DispalyNameUser} };
            if (fullObject.Categories.Count > 0)
            {
                collection.Add(new RequestCategoriesPayload() { Categories = fullObject.Categories });
            }
                
            return collection;
        }

        public event GenericEventHandler<FamilyKeyPerimetersAddedEventArgs> FamilyKeyPerimetersAdded;
        public event GenericEventHandler<AbonentPerimetersTariffChangedEventArgs> PerimetersTariffChanged;
        public event GenericEventHandler<AbonentPerimetersRemovedEventArgs> PerimetersRemoved;
        public event GenericEventHandler<AbonentCarsChangedEventArgs> CarsChanged;

        public event GenericEventHandler<AbonentAttributeChangedEventArgs> AbonentAttributeChanged;

        public event GenericEventHandler<TemporaryKeyPerimetersAddedEventArgs> TemporaryKeyPerimetersAdded;

        
    }
}
