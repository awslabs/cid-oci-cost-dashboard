CREATE OR REPLACE VIEW "focus_consolidation_view" AS 
SELECT
  AvailabilityZone
, BilledCost
, BillingAccountId
, BillingAccountName
, BillingCurrency
, BillingPeriodEnd
, BillingPeriodStart
, ChargeCategory
, ChargeClass
, ChargeDescription
, ChargeFrequency
, ChargePeriodEnd
, ChargePeriodStart
, CommitmentDiscountCategory
, CommitmentDiscountId
, CommitmentDiscountName
, CommitmentDiscountType
, CommitmentDiscountStatus
, ConsumedQuantity
, ConsumedUnit
, ContractedCost
, ContractedUnitPrice
, EffectiveCost
, InvoiceIssuerName
, ListCost
, ListUnitPrice
, PricingCategory
, PricingQuantity
, PricingUnit
, ProviderName
, PublisherName
, RegionId
, RegionName
, ResourceId
, ResourceName
, ResourceType
, ServiceCategory
, ServiceName
, SkuId
, SkuPriceId
, SubAccountId
, SubAccountName
, Tags
, billing_period
FROM
  "cid_data_export"."focus"
UNION ALL SELECT
  null AvailabilityZone
, BilledCost
, BillingAccountId
, BillingAccountName
, BillingCurrency
, BillingPeriodEnd
, BillingPeriodStart
, ChargeCategory
, ChargeClass
, ChargeDescription
, ChargeFrequency
, ChargePeriodEnd
, ChargePeriodStart
, CommitmentDiscountCategory
, CommitmentDiscountId
, CommitmentDiscountName
, CommitmentDiscountType
, CommitmentDiscountStatus
, ConsumedQuantity
, ConsumedUnit
, ContractedCost
, ContractedUnitPrice
, EffectiveCost
, InvoiceIssuerName
, ListCost
, ListUnitPrice
, PricingCategory
, PricingQuantity
, PricingUnit
, ProviderName
, PublisherName
, RegionId
, RegionName
, ResourceId
, ResourceName
, ResourceType
, ServiceCategory
, ServiceName
, SkuId
, SkuPriceId
, SubAccountId
, SubAccountName
, Tags
, billing_period
FROM
  "cidgldpdcidazure"."cidgltpdcidazure"
UNION ALL SELECT
  availabilityzone AvailabilityZone
, BilledCost
, BillingAccountId
, BillingAccountName
, BillingCurrency
, BillingPeriodEnd
, BillingPeriodStart
, ChargeCategory
, null ChargeClass
, ChargeDescription
, ChargeFrequency
, ChargePeriodEnd
, ChargePeriodStart
, CommitmentDiscountCategory
, CommitmentDiscountId
, CommitmentDiscountName
, CommitmentDiscountType
, null CommitmentDiscountStatus
, null ConsumedQuantity
, null ConsumedUnit
, null ContractedCost
, null ContractedUnitPrice
, EffectiveCost
, invoiceissuer InvoiceIssuerName
, ListCost
, ListUnitPrice
, PricingCategory
, PricingQuantity
, PricingUnit
, provider ProviderName
, publisher PublisherName
, region RegionId
, region RegionName
, ResourceId
, ResourceName
, ResourceType
, ServiceCategory
, ServiceName
, SkuId
, SkuPriceId
, SubAccountId
, SubAccountName
, null Tags
, billing_period
FROM
  "cid_oci_focus_data_export_dv"."focus"
