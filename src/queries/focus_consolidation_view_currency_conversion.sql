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
  AvailabilityZone
, (CASE WHEN (oci.BillingCurrency <> 'USD') THEN (oci.BilledCost * COALESCE(cr.ConversionToUSDRate, 1)) ELSE oci.BilledCost END) BilledCost
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
, (CASE WHEN (oci.BillingCurrency <> 'USD') THEN (oci.EffectiveCost * COALESCE(cr.ConversionToUSDRate, 1)) ELSE oci.EffectiveCost END) EffectiveCost
, null InvoiceIssuerName
, (CASE WHEN (oci.BillingCurrency <> 'USD') THEN (oci.ListCost * COALESCE(cr.ConversionToUSDRate, 1)) ELSE oci.ListCost END) ListCost
, (CASE WHEN (oci.BillingCurrency <> 'USD') THEN (oci.ListUnitPrice * COALESCE(cr.ConversionToUSDRate, 1)) ELSE oci.ListUnitPrice END) ListUnitPrice
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
, Tags
, billing_period
FROM
  ("cid_oci_data_export_ws"."focus-oci" oci
LEFT JOIN "cid_oci_focus_data_export_ck1"."currency_rates" cr ON ((oci.BillingCurrency = cr.CurrencyName) AND (DATE(oci.BillingPeriodStart) = cr.Date)))
