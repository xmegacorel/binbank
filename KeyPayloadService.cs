using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AutoMapper;
using Serilog;
using SmartAirKey.Core;
using SmartAirKey.Core.Events;
using SmartAirKey.KeyServer.Customers;
using SmartAirKey.KeyServer.Intercoms.Abonents.Events;
using SmartAirKey.KeyServer.Intercoms.Keys;
using SmartAirKey.KeyServer.Intercoms.ServiceCompanies;
using SmartAirKey.KeyServer.Keys;
using SmartAirKey.KeyServer.Keys.Renewal;
using SmartAirKey.KeyServer.Keys.Templates;
using SmartAirKey.KeyServer.SharedKernel;
using SmartAirKey.KeyServer.Territory.AccessPerimeters;
using SmartAirKey.KeyServer.Users;

namespace SmartAirKey.KeyServer.Intercoms.Abonents
{
    public class KeyPayloadService
    {
        private readonly IAbonentService _abonentService;
        private readonly UserCompositeKeyRepository _userCompositeKeyRepository;
        private readonly ICompositeKeyTemplateService _compositeKeyTemplateService;
        private readonly IUserKeyRenewalService _userKeyRenewalService;
        private readonly IAccessPerimeterKeyService _accessPerimeterKeyService;

        public KeyPayloadService(
            IAbonentService abonentService, 
            UserCompositeKeyRepository userCompositeKeyRepository,
            ICompositeKeyTemplateService compositeKeyTemplateService,
            IUserKeyRenewalService userKeyRenewalService,
            IAccessPerimeterKeyService accessPerimeterKeyService)
        {
            _abonentService = abonentService;
            _userCompositeKeyRepository = userCompositeKeyRepository;
            _compositeKeyTemplateService = compositeKeyTemplateService;
            _userKeyRenewalService = userKeyRenewalService;
            _accessPerimeterKeyService = accessPerimeterKeyService;
        }

        public void Start()
        {
            _abonentService.CarsChanged += EventHelper.Handle<AbonentCarsChangedEventArgs>(AbonentCarsChangedHandlerAsync);
            _abonentService.AbonentAttributeChanged += EventHelper.Handle<AbonentAttributeChangedEventArgs>(AbonentAttributeHandlerAsync);
        }

        private async Task<Result> AbonentAttributeHandlerAsync(AbonentAttributeChangedEventArgs args)
        {
            Log.Debug("Updating payload in keys by user: {UserId} and Customer {CustomerId} Payload {Payload}", args.UserId, args.CompanyId, args.Payload);

            var keys = await GetFamilyAndTemporaryKeys(args.PerimeterIds, args.CompanyId, args.UserId);
            var memberKeys = await _userCompositeKeyRepository.GetAllMemberKeysByOwnerKeyIdsAsync(args.UserId, keys.Select(t => t.ToIdentity()));
            var keysToUpdate = keys.Union(memberKeys);

            Log.Debug("Applying payload update for {UserId} changed by {CustomerId}: {KeyIds}", keys.Count, args.UserId, args.CompanyId, keysToUpdate.Select(x => x.Id));

            foreach (var key in keysToUpdate)
            {
                await UpdatePayloadAsync(args, key);

                await _userKeyRenewalService.ProcessAsync(new ProlongateUserKeyRequest()
                {
                    UserId = args.UserId,
                    CompositeKeyId = key.Id.ToIdentity<UserCompositeKey>()
                });
            }

            return Result.Ok();
        }

        private async Task<List<UserCompositeKey>> GetFamilyAndTemporaryKeys(
            IEnumerable<Identity<AccessPerimeter>> perimeterIds, Identity<ServiceCompany> companyId, Identity<User> userId)
        {
            var templateIds = await _accessPerimeterKeyService.GetTemplatesByPerimeterIdsAsync(perimeterIds, companyId);
            // получить все ключи по владельцу, отфильтровать по СК, типу ключа и шаблону
            // взять все шаблоны из периметров
            var allUserKey = await _userCompositeKeyRepository.GetAllByOwnerIdAsync(userId);

            var result = allUserKey.Where(t => t.CustomerId == companyId.To<Customer>() &&
                                  (t.Type == UserSmartKeyType.Family || t.Type == UserSmartKeyType.Temporary) &&
                                  templateIds.Contains(t.TemplateId)).ToList();

            return result;
        }

        private Task UpdatePayloadAsync(AbonentAttributeChangedEventArgs args, UserCompositeKey key)
        {
            foreach (var arg in args.Payload)
            {
                var value = Mapper.Map<PayloadItemBase>(arg);
                var typePayload = value.GetType();
                var item = key.Payload.SingleOrDefault(t => t.GetType() == typePayload);

                if (item != null)
                {
                    key.Payload.Remove(item);
                }

                key.Payload.Add(value);
            }

            return _userCompositeKeyRepository.UpdatePayloadAsync(key.ToIdentity(), key.Payload);
        }

        private async Task<Result> AbonentCarsChangedHandlerAsync(AbonentCarsChangedEventArgs args)
        {
            Log.Debug("Updating cars in keys by user: {UserId} and Customer {CustomerId}", args.UserId, args.CompanyId);

            // обновлять только уже выпущенные ключи
            var keys = await GetParkingKeysByUserAndCustomerAsync(args);
            var memberKeys = await _userCompositeKeyRepository.GetAllMemberKeysByOwnerKeyIdsAsync(args.UserId, keys.Select(t => t.ToIdentity()));
            var keysToUpdate = keys.Union(memberKeys);

            Log.Debug("Applying cars payload update for {UserId} changed by {CustomerId}: {KeyIds}", keys.Count, args.UserId, args.CompanyId, keysToUpdate.Select(x => x.Id));

            foreach (var key in keysToUpdate)
            {
                if (key.Payload.GetTypedPayload<CarPayloadItem>().HasNoValue)
                    continue;

                await UpdateCarsPayloadAsync(args, key);
            }
            
            return Result.Ok();
        }

        private async Task<List<UserCompositeKey>> GetParkingKeysByUserAndCustomerAsync(AbonentCarsChangedEventArgs args)
        {
            var keys = await GetFamilyAndTemporaryKeys(args.PerimeterIds, args.CompanyId, args.UserId);

            var templates = await _compositeKeyTemplateService.FilterAsync(keys.Select(t => t.TemplateId));

            if (templates == null || templates.Count == 0)
                return new List<UserCompositeKey>();

            var result = from t in keys
                         join m in templates on t.TemplateId equals m.ToIdentity()
                where m.IsParking
                select t;

            return result.ToList();
        }

        private Task UpdateCarsPayloadAsync(AbonentCarsChangedEventArgs args, UserCompositeKey key)
        {
            var cars = (key.Payload.GetTypedPayload<CarPayloadItem>().Value).Cars;

            if (args.Deleted.Any())
            {
                var deleted = args.Deleted.Select(t => t.ToString());
                for (int i = cars.Count - 1; i >= 0; i--)
                {
                    var item = cars[i];
                    if (deleted.Contains(item))
                    {
                        cars.RemoveAt(i);
                    }
                }
            }

            if (args.Added.Any())
            {
                foreach (var car in args.Added)
                {
                    cars.Add(car.ToString());
                }
            }

            return _userCompositeKeyRepository.UpdatePayloadAsync(key.ToIdentity(), key.Payload);
        }
    }
}