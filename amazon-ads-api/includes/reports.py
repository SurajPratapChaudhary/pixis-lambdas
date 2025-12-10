import re, os
from includes.constants import Constant

GENERAL_TABLES = {
    Constant.TBL_TYPE_GENERAL: {
        # Constant.TBL_BID_ADJUSTMENT_CONFIG: {
        #     'partitionColumn': None,
        #     'clusterColumn': ['client_id', 'profileId'],
        #     'loadMethod': 'merge',
        #     'mergeColumns': ['id', 'client_id', 'profileId'],
        #     'dataset': 'general',
        #     'tableName': Constant.TBL_BID_ADJUSTMENT_CONFIG,
        #     'schema': {
        #         "id": "STRING",
        #         "client_id": "INT64",
        #         "profileId": "STRING",
        #         "portfolioId": "INT64",
        #         "portfolioName": "STRING",
        #         "campaignId": "INT64",
        #         "campaignName": "STRING",
        #         "target_acos": "FLOAT64",
        #         "above_acos_range": "FLOAT64",
        #         "below_acos_range": "FLOAT64",
        #         "max_impressions": "INT64",
        #         "increase_bid_by": "FLOAT64",
        #         "decrease_bid_by": "FLOAT64",
        #         "asin_aov": "FLOAT64",
        #         "asin_spend_threshold": "FLOAT64",
        #         "asin_max_bid": "FLOAT64",
        #         "asin_bid_adjustment_governor": "FLOAT64",
        #         "expected_multiplier": "FLOAT64",
        #         "max_bid_decrease": "FLOAT64",
        #         "max_bid_increase": "FLOAT64",
        #         "last_updated": "TIMESTAMP",
        #     }
        # }
    }
}

ENTITIES = {
    Constant.ADTYPE_SP: {
        Constant.ADS_PORTFOLIO: {
            'partitionColumn': None,
            'clusterColumn': ['profileId', 'portfolioId'],
            'loadMethod': 'delete_insert',
            'mergeColumns': ['profileId'],
            'dataset': 'ads_sp',
            'tableName': 'entities_portfolio',
            'schema': {
                "profileId": "STRING",
                "last_updated": "TIMESTAMP",
                "portfolioId": "INT64",
                "name": "STRING",
                "state": "STRING",
                "inBudget": "BOOLEAN",
                "currencyCode": "STRING",
                "policy": "STRING",
                "budgetControls": "STRING",
                "creationDateTime": "TIMESTAMP",
                "lastUpdateDateTime": "TIMESTAMP",
                "servingStatus": "STRING",
                "servingStatusDetails": "STRING"
            }
        },
        Constant.ADS_CAMPAIGN: {
            'partitionColumn': None,
            'clusterColumn': ['profileId', 'campaignId'],
            'loadMethod': 'delete_insert',
            'mergeColumns': ['profileId'],
            'dataset': 'ads_sp',
            'tableName': 'entities_campaign',
            'schema': {
                "profileId": "STRING",
                "last_updated": "TIMESTAMP",
                "campaignId": "INT64",
                "name": "STRING",
                "state": "STRING",
                "budget": "FLOAT64",
                "budgetType": "STRING",
                "dynamicBidding": "STRING",
                "startDate": "TIMESTAMP",
                "offAmazonSettings": "STRING",
                "portfolioId": "INT64",
                "tags": "STRING",
                "targetingType": "STRING",
                "creationDateTime": "TIMESTAMP",
                "lastUpdateDateTime": "TIMESTAMP",
                "servingStatus": "STRING",
                "servingStatusDetails": "STRING"
            }
        },
        Constant.ADS_ADGROUP: {
            'partitionColumn': None,
            'clusterColumn': ['profileId', 'adGroupId', 'campaignId'],
            'loadMethod': 'delete_insert',
            'mergeColumns': ['profileId'],
            'dataset': 'ads_sp',
            'tableName': 'entities_adgroup',
            'schema': {
                "profileId": "STRING",
                "last_updated": "TIMESTAMP",
                "adGroupId": "INT64",
                "campaignId": "INT64",
                "name": "STRING",
                "state": "STRING",
                "defaultBid": "FLOAT64",
                "creationDateTime": "TIMESTAMP",
                "lastUpdateDateTime": "TIMESTAMP",
                "servingStatus": "STRING",
                "servingStatusDetails": "STRING"
            }
        },
        Constant.ADS_KEYWORD: {
            'partitionColumn': None,
            'clusterColumn': ['profileId', 'keywordId', 'adGroupId', 'campaignId'],
            'loadMethod': 'delete_insert',
            'mergeColumns': ['profileId'],
            'dataset': 'ads_sp',
            'tableName': 'entities_keyword',
            'schema': {
                "profileId": "STRING",
                "last_updated": "TIMESTAMP",
                "keywordId": "INT64",
                "keywordText": "STRING",
                "matchType": "STRING",
                "state": "STRING",
                "bid": "FLOAT64",
                "adGroupId": "INT64",
                "campaignId": "INT64",
                "creationDateTime": "TIMESTAMP",
                "lastUpdateDateTime": "TIMESTAMP",
                "servingStatus": "STRING",
                "servingStatusDetails": "STRING",
            }
        },
        Constant.ADS_TARGET: {
            'partitionColumn': None,
            'clusterColumn': ['profileId', 'targetId', 'adGroupId', 'campaignId'],
            'loadMethod': 'delete_insert',
            'mergeColumns': ['profileId'],
            'dataset': 'ads_sp',
            'tableName': 'entities_target',
            'schema': {
                "profileId": "STRING",
                "last_updated": "TIMESTAMP",
                "targetId": "INT64",
                "expression": "STRING",
                "expressionType": "STRING",
                "resolvedExpression": "STRING",
                "state": "STRING",
                "bid": "FLOAT64",
                "adGroupId": "INT64",
                "campaignId": "INT64",
                "creationDateTime": "TIMESTAMP",
                "lastUpdateDateTime": "TIMESTAMP",
                "servingStatus": "STRING",
                "servingStatusDetails": "STRING",
            }
        },
        Constant.ADS_PRODUCTAD: {
            'partitionColumn': None,
            'clusterColumn': ['profileId', 'adId', 'adGroupId', 'campaignId'],
            'loadMethod': 'delete_insert',
            'mergeColumns': ['profileId'],
            'dataset': 'ads_sp',
            'tableName': 'entities_productad',
            'schema': {
                "profileId": "STRING",
                "last_updated": "TIMESTAMP",
                "adId": "INT64",
                "asin": "STRING",
                "sku": "STRING",
                "state": "STRING",
                "adGroupId": "INT64",
                "campaignId": "INT64",
                "creationDateTime": "TIMESTAMP",
                "lastUpdateDateTime": "TIMESTAMP",
                "servingStatus": "STRING",
                "servingStatusDetails": "STRING",
            }
        }
    },
    Constant.ADTYPE_SB: {
        Constant.ADS_CAMPAIGN: {
            'partitionColumn': None,
            'clusterColumn': ['profileId', 'campaignId'],
            'loadMethod': 'delete_insert',
            'mergeColumns': ['profileId'],
            'dataset': 'ads_sb',
            'tableName': 'entities_campaign',
            'schema': {
                "profileId": "STRING",
                "last_updated": "TIMESTAMP",
                "campaignId": "INT64",
                "name": "STRING",
                "state": "STRING",
                "costType": "STRING",
                "brandEntityId": "STRING",
                "budget": "FLOAT64",
                "budgetType": "STRING",
                "bidding": "STRING",
                "goal": "STRING",
                "isMultiAdGroupsEnabled": "BOOLEAN",
                "kpi": "STRING",
                "smartDefault": "STRING",
                "startDate": "TIMESTAMP",
                "creationDateTime": "TIMESTAMP",
                "lastUpdateDateTime": "TIMESTAMP",
                "servingStatus": "STRING",
                "servingStatusDetails": "STRING",
            }
        },
        Constant.ADS_ADGROUP: {
            'partitionColumn': None,
            'clusterColumn': ['profileId', 'adGroupId', 'campaignId'],
            'loadMethod': 'delete_insert',
            'mergeColumns': ['profileId'],
            'dataset': 'ads_sb',
            'tableName': 'entities_adgroup',
            'schema': {
                "profileId": "STRING",
                "last_updated": "TIMESTAMP",
                "adGroupId": "INT64",
                "campaignId": "INT64",
                "name": "STRING",
                "state": "STRING",
                "defaultBid": "FLOAT64",
                "creationDateTime": "TIMESTAMP",
                "lastUpdateDateTime": "TIMESTAMP",
                "servingStatus": "STRING",
                "servingStatusDetails": "STRING",
            }
        },
        Constant.ADS_KEYWORD: {
            'partitionColumn': None,
            'clusterColumn': ['profileId', 'keywordId', 'adGroupId', 'campaignId'],
            'loadMethod': 'delete_insert',
            'mergeColumns': ['profileId'],
            'dataset': 'ads_sb',
            'tableName': 'entities_keyword',
            'schema': {
                "profileId": "STRING",
                "last_updated": "TIMESTAMP",
                "keywordId": "INT64",
                "keywordText": "STRING",
                "matchType": "STRING",
                "state": "STRING",
                "bid": "FLOAT64",
                "adGroupId": "INT64",
                "campaignId": "INT64",
            }
        },
        Constant.ADS_TARGET: {
            'partitionColumn': None,
            'clusterColumn': ['profileId', 'targetId', 'adGroupId', 'campaignId'],
            'loadMethod': 'delete_insert',
            'mergeColumns': ['profileId'],
            'dataset': 'ads_sb',
            'tableName': 'entities_target',
            'schema': {
                "profileId": "STRING",
                "last_updated": "TIMESTAMP",
                "targetId": "INT64",
                "expressions": "STRING",
                "resolvedExpressions": "STRING",
                "state": "STRING",
                "bid": "FLOAT64",
                "adGroupId": "INT64",
                "campaignId": "INT64",
            }
        },
        Constant.ADS_AD: {
            'partitionColumn': None,
            'clusterColumn': ['profileId', 'adId', 'adGroupId', 'campaignId'],
            'loadMethod': 'delete_insert',
            'mergeColumns': ['profileId'],
            'dataset': 'ads_sb',
            'tableName': 'entities_ad',
            'schema': {
                "profileId": "STRING",
                "last_updated": "TIMESTAMP",
                "adId": "INT64",
                "name": "STRING",
                "state": "STRING",
                "adGroupId": "INT64",
                "campaignId": "INT64",
                "creative": "STRING",
                "landingPage": "STRING",
                "servingStatus": "STRING",
                "servingStatusDetails": "STRING",
                "creationDateTime": "TIMESTAMP",
                "lastUpdateDateTime": "TIMESTAMP",
            }
        },
    },
    Constant.ADTYPE_SD: {
        Constant.ADS_CAMPAIGN: {
            'partitionColumn': None,
            'clusterColumn': ['profileId', 'campaignId'],
            'loadMethod': 'delete_insert',
            'mergeColumns': ['profileId'],
            'dataset': 'ads_sd',
            'tableName': 'entities_campaign',
            'schema': {
                "profileId": "STRING",
                "last_updated": "TIMESTAMP",
                "campaignId": "INT64",
                "portfolioId": "INT64",
                "name": "STRING",
                "tactic": "STRING",
                "startDate": "TIMESTAMP",
                "state": "STRING",
                "costType": "STRING",
                "budget": "FLOAT64",
                "ruleBasedBudget": "STRING",
                "budgetType": "STRING",
                "deliveryProfile":"STRING"
            }
        },
        Constant.ADS_ADGROUP: {
            'partitionColumn': None,
            'clusterColumn': ['profileId', 'adGroupId', 'campaignId'],
            'loadMethod': 'delete_insert',
            'mergeColumns': ['profileId'],
            'dataset': 'ads_sd',
            'tableName': 'entities_adgroup',
            'schema': {
                "profileId": "STRING",
                "last_updated": "TIMESTAMP",
                "adGroupId": "INT64",
                "campaignId": "INT64",
                "name": "STRING",
                "state": "STRING",
                "defaultBid": "FLOAT64",
                "bidOptimization": "STRING",
                "tactic": "STRING",
                "creativeType": "STRING"
            }
        },
        Constant.ADS_TARGET: {
            'partitionColumn': None,
            'clusterColumn': ['profileId', 'targetId', 'adGroupId', 'campaignId'],
            'loadMethod': 'delete_insert',
            'mergeColumns': ['profileId'],
            'dataset': 'ads_sd',
            'tableName': 'entities_target',
            'schema': {
                "profileId": "STRING",
                "last_updated": "TIMESTAMP",
                "targetId": "INT64",
                "expression": "STRING",
                "expressionType": "STRING",
                "resolvedExpression": "STRING",
                "state": "STRING",
                "bid": "FLOAT64",
                "adGroupId": "INT64",
                "campaignId": "INT64",
            }
        },
        Constant.ADS_PRODUCTAD: {
            'partitionColumn': None,
            'clusterColumn': ['profileId', 'adId', 'adGroupId', 'campaignId'],
            'loadMethod': 'delete_insert',
            'mergeColumns': ['profileId'],
            'dataset': 'ads_sd',
            'tableName': 'entities_productad',
            'schema': {
                "profileId": "STRING",
                "last_updated": "TIMESTAMP",
                "adId": "INT64",
                "adName": "STRING",
                "asin":"STRING",
                "sku":"STRING",
                "state": "STRING",
                "adGroupId": "INT64",
                "campaignId": "INT64",
                "landingPageURL":"STRING",
                "landingPageType":"STRING",
            }
        }
    },
}

REPORTS = {
    Constant.ADTYPE_SP: {
        Constant.ADS_CAMPAIGN: {
            'partitionColumn': 'date',
            'clusterColumn': ['profileId', 'campaignId'],
            'loadMethod': 'merge',
            'mergeColumns': ['marketplace', 'profileId', 'date', 'campaignId'],
            'dataset': 'ads_sp',
            'tableName': 'reports_campaign',
            'schema': {
                "marketplace": "STRING",
                "profileId": "STRING",
                "date": "TIMESTAMP",
                "last_updated": "TIMESTAMP",

                "impressions": "INT64",
                "clicks": "INT64",
                "cost": "FLOAT64",
                "purchases1d": "INT64",
                "purchases7d": "INT64",
                "purchases14d": "INT64",
                "purchases30d": "INT64",
                "purchasesSameSku1d": "INT64",
                "purchasesSameSku7d": "INT64",
                "purchasesSameSku14d": "INT64",
                "purchasesSameSku30d": "INT64",
                "unitsSoldClicks1d": "INT64",
                "unitsSoldClicks7d": "INT64",
                "unitsSoldClicks14d": "INT64",
                "unitsSoldClicks30d": "INT64",
                "sales1d": "FLOAT64",
                "sales7d": "FLOAT64",
                "sales14d": "FLOAT64",
                "sales30d": "FLOAT64",
                "attributedSalesSameSku1d": "FLOAT64",
                "attributedSalesSameSku7d": "FLOAT64",
                "attributedSalesSameSku14d": "FLOAT64",
                "attributedSalesSameSku30d": "FLOAT64",
                "unitsSoldSameSku1d": "INT64",
                "unitsSoldSameSku7d": "INT64",
                "unitsSoldSameSku14d": "INT64",
                "unitsSoldSameSku30d": "INT64",
                "kindleEditionNormalizedPagesRead14d": "INT64",
                "kindleEditionNormalizedPagesRoyalties14d": "INT64",
                "campaignBiddingStrategy": "STRING",
                "costPerClick": "FLOAT64",
                "clickThroughRate": "FLOAT64",
                "spend": "FLOAT64",
                "campaignName": "STRING",
                "campaignId": "INT64",
                "campaignStatus": "STRING",
                "campaignBudgetAmount": "FLOAT64",
                "campaignBudgetType": "STRING",
                "campaignRuleBasedBudgetAmount": "FLOAT64",
                "campaignApplicableBudgetRuleId": "STRING",
                "campaignApplicableBudgetRuleName": "STRING",
                "campaignBudgetCurrencyCode": "STRING",
                "topOfSearchImpressionShare": "FLOAT64"
            }
        },
        Constant.ADS_ADGROUP: {
            'partitionColumn': 'date',
            'clusterColumn': ['profileId', 'campaignId', 'adGroupId'],
            'loadMethod': 'merge',
            'mergeColumns': ['marketplace', 'profileId', 'date', 'campaignId', 'adGroupId'],
            'dataset': 'ads_sp',
            'tableName': 'reports_adgroup',
            'schema': {
                "marketplace": "STRING",
                "profileId": "STRING",
                "date": "TIMESTAMP",
                "last_updated": "TIMESTAMP",

                "impressions": "INT64",
                "clicks": "INT64",
                "cost": "FLOAT64",
                "purchases1d": "INT64",
                "purchases7d": "INT64",
                "purchases14d": "INT64",
                "purchases30d": "INT64",
                "purchasesSameSku1d": "INT64",
                "purchasesSameSku7d": "INT64",
                "purchasesSameSku14d": "INT64",
                "purchasesSameSku30d": "INT64",
                "unitsSoldClicks1d": "INT64",
                "unitsSoldClicks7d": "INT64",
                "unitsSoldClicks14d": "INT64",
                "unitsSoldClicks30d": "INT64",
                "sales1d": "FLOAT64",
                "sales7d": "FLOAT64",
                "sales14d": "FLOAT64",
                "sales30d": "FLOAT64",
                "attributedSalesSameSku1d": "FLOAT64",
                "attributedSalesSameSku7d": "FLOAT64",
                "attributedSalesSameSku14d": "FLOAT64",
                "attributedSalesSameSku30d": "FLOAT64",
                "unitsSoldSameSku1d": "INT64",
                "unitsSoldSameSku7d": "INT64",
                "unitsSoldSameSku14d": "INT64",
                "unitsSoldSameSku30d": "INT64",
                "kindleEditionNormalizedPagesRead14d": "INT64",
                "kindleEditionNormalizedPagesRoyalties14d": "INT64",
                "campaignBiddingStrategy": "STRING",
                "costPerClick": "FLOAT64",
                "clickThroughRate": "FLOAT64",
                "spend": "FLOAT64",
                "campaignName": "STRING",
                "campaignId": "INT64",
                "campaignStatus": "STRING",
                "campaignBudgetAmount": "FLOAT64",
                "campaignBudgetType": "STRING",
                "campaignRuleBasedBudgetAmount": "FLOAT64",
                "campaignApplicableBudgetRuleId": "STRING",
                "campaignApplicableBudgetRuleName": "STRING",
                "campaignBudgetCurrencyCode": "STRING",
                "topOfSearchImpressionShare": "FLOAT64",
                "adGroupName": "STRING",
                "adGroupId": "INT64",
                "adStatus": "STRING"
            }
        },
        Constant.ADS_TARGET: {
            'partitionColumn': 'date',
            'clusterColumn': ['profileId', 'keywordId', 'date'],
            'loadMethod': 'merge',
            'mergeColumns': ['marketplace', 'profileId', 'date', 'keywordId', 'adGroupId', 'campaignId'],
            'dataset': 'ads_sp',
            'tableName': 'reports_target',
            'schema': {
                "marketplace": "STRING",
                "profileId": "STRING",
                "date": "TIMESTAMP",
                "last_updated": "TIMESTAMP",

                "keywordId": "INT64",
                "keyword": "STRING",
                "keywordType": "STRING",
                "keywordBid": "FLOAT64",
                "matchType": "STRING",
                "targeting": "STRING",
                "adKeywordStatus": "STRING",

                "campaignId": "INT64",
                "campaignName": "STRING",
                "campaignStatus": "STRING",
                "campaignBudgetType": "STRING",
                "campaignBudgetAmount": "FLOAT64",
                "campaignBudgetCurrencyCode": "STRING",

                "adGroupId": "INT64",
                "adGroupName": "STRING",

                "attributedSalesSameSku1d": "FLOAT64",
                "roasClicks14d": "FLOAT64",
                "unitsSoldClicks1d": "INT64",
                "attributedSalesSameSku14d": "FLOAT64",
                "sales7d": "FLOAT64",
                "attributedSalesSameSku30d": "FLOAT64",
                "kindleEditionNormalizedPagesRoyalties14d": "INT64",
                "unitsSoldSameSku1d": "INT64",
                "salesOtherSku7d": "FLOAT64",
                "purchasesSameSku7d": "INT64",
                "purchases7d": "INT64",
                "unitsSoldSameSku30d": "INT64",
                "costPerClick": "FLOAT64",
                "unitsSoldClicks14d": "INT64",
                "clickThroughRate": "FLOAT64",
                "kindleEditionNormalizedPagesRead14d": "INT64",
                "acosClicks14d": "FLOAT64",
                "unitsSoldClicks30d": "INT64",
                "qualifiedBorrows": "INT64",
                "portfolioId": "INT64",
                "roasClicks7d": "FLOAT64",
                "unitsSoldSameSku14d": "INT64",
                "unitsSoldClicks7d": "INT64",
                "attributedSalesSameSku7d": "FLOAT64",
                "topOfSearchImpressionShare": "FLOAT64",
                "royaltyQualifiedBorrows": "INT64",
                "sales1d": "FLOAT64",

                "addToList": "INT64",
                "purchasesSameSku14d": "INT64",
                "unitsSoldOtherSku7d": "INT64",
                "purchasesSameSku1d": "INT64",
                "purchases1d": "INT64",
                "unitsSoldSameSku7d": "INT64",
                "cost": "FLOAT64",
                "sales14d": "FLOAT64",
                "acosClicks7d": "FLOAT64",
                "sales30d": "FLOAT64",
                "impressions": "INT64",
                "purchasesSameSku30d": "INT64",
                "purchases14d": "INT64",
                "purchases30d": "INT64",
                "clicks": "INT64"
            }
        },
        Constant.ADS_KEYWORD: {
            'partitionColumn': 'date',
            'clusterColumn': ['profileId', 'keywordId', 'date'],
            'loadMethod': 'merge',
            'mergeColumns': ['marketplace', 'profileId', 'date', 'keywordId', 'adGroupId', 'campaignId'],
            'dataset': 'ads_sp',
            'tableName': 'reports_keyword',
            'schema': {
                "marketplace": "STRING",
                "profileId": "STRING",
                "date": "TIMESTAMP",
                "last_updated": "TIMESTAMP",

                "keywordId": "INT64",
                "keyword": "STRING",
                "keywordType": "STRING",
                "keywordBid": "FLOAT64",
                "matchType": "STRING",
                "targeting": "STRING",
                "adKeywordStatus": "STRING",

                "campaignId": "INT64",
                "campaignName": "STRING",
                "campaignStatus": "STRING",
                "campaignBudgetType": "STRING",
                "campaignBudgetAmount": "FLOAT64",
                "campaignBudgetCurrencyCode": "STRING",

                "adGroupId": "INT64",
                "adGroupName": "STRING",

                "attributedSalesSameSku1d": "FLOAT64",
                "roasClicks14d": "FLOAT64",
                "unitsSoldClicks1d": "INT64",
                "attributedSalesSameSku14d": "FLOAT64",
                "sales7d": "FLOAT64",
                "attributedSalesSameSku30d": "FLOAT64",
                "kindleEditionNormalizedPagesRoyalties14d": "INT64",
                "unitsSoldSameSku1d": "INT64",
                "salesOtherSku7d": "FLOAT64",
                "purchasesSameSku7d": "INT64",
                "purchases7d": "INT64",
                "unitsSoldSameSku30d": "INT64",
                "costPerClick": "FLOAT64",
                "unitsSoldClicks14d": "INT64",
                "clickThroughRate": "FLOAT64",
                "kindleEditionNormalizedPagesRead14d": "INT64",
                "acosClicks14d": "FLOAT64",
                "unitsSoldClicks30d": "INT64",
                "qualifiedBorrows": "INT64",
                "portfolioId": "INT64",
                "roasClicks7d": "FLOAT64",
                "unitsSoldSameSku14d": "INT64",
                "unitsSoldClicks7d": "INT64",
                "attributedSalesSameSku7d": "FLOAT64",
                "topOfSearchImpressionShare": "FLOAT64",
                "royaltyQualifiedBorrows": "INT64",
                "sales1d": "FLOAT64",

                "addToList": "INT64",
                "purchasesSameSku14d": "INT64",
                "unitsSoldOtherSku7d": "INT64",
                "purchasesSameSku1d": "INT64",
                "purchases1d": "INT64",
                "unitsSoldSameSku7d": "INT64",
                "cost": "FLOAT64",
                "sales14d": "FLOAT64",
                "acosClicks7d": "FLOAT64",
                "sales30d": "FLOAT64",
                "impressions": "INT64",
                "purchasesSameSku30d": "INT64",
                "purchases14d": "INT64",
                "purchases30d": "INT64",
                "clicks": "INT64"
            }
        },
    },
    Constant.ADTYPE_SB: {
        Constant.ADS_CAMPAIGN: {
            'partitionColumn': 'date',
            'loadMethod': 'merge',
            'mergeColumns': ['marketplace', 'profileId', 'date', 'campaignId'],
            'dataset': 'ads_sb',
            'tableName': 'reports_campaign',
            'schema': {
                "marketplace": "STRING",
                "profileId": "STRING",
                "date": "TIMESTAMP",
                "last_updated": "TIMESTAMP",
        
                "newToBrandSalesClicks": "FLOAT64",
                "viewabilityRate": "FLOAT64",
                "purchasesClicks": "INT64",
                "detailPageViews": "INT64",
                "newToBrandSalesPercentage": "FLOAT64",
                "unitsSold": "INT64",
                "sales": "FLOAT64",
                "costType": "STRING",
                "campaignStatus": "STRING",
                "viewClickThroughRate": "FLOAT64",
                "campaignBudgetAmount": "FLOAT64",
                "purchasesPromoted": "INT64",
                "newToBrandDetailPageViewRate": "FLOAT64",
                "purchases": "INT64",
                "campaignId": "INT64",
                "newToBrandPurchasesPercentage": "FLOAT64",
                "salesPromoted": "FLOAT64",
                "campaignBudgetCurrencyCode": "STRING",
                "videoCompleteViews": "INT64",
                "salesClicks": "FLOAT64",
                "videoFirstQuartileViews": "INT64",
                "newToBrandPurchasesRate": "FLOAT64",
                "unitsSoldClicks": "INT64",
                "brandedSearches": "INT64",
                "video5SecondViews": "INT64",
                "topOfSearchImpressionShare": "FLOAT64",
                "newToBrandECPDetailPageView": "FLOAT64",
                "newToBrandPurchases": "INT64",
                "brandedSearchesClicks": "INT64",
                "newToBrandSales": "FLOAT64",
                "newToBrandPurchasesClicks": "INT64",
                "campaignBudgetType": "STRING",
                "addToCartClicks": "INT64",
                "videoUnmutes": "INT64",
                "addToCart": "FLOAT64",
                "videoThirdQuartileViews": "INT64",
                "addToCartRate": "FLOAT64",
                "cost": "FLOAT64",
                "newToBrandDetailPageViewsClicks": "INT64",
                "impressions": "INT64",
                "newToBrandUnitsSold": "INT64",
                "viewableImpressions": "INT64",
                "detailPageViewsClicks": "INT64",
                "eCPAddToCart": "FLOAT64",
                "newToBrandDetailPageViews": "INT64",
                "clicks": "INT64",
                "videoMidpointViews": "INT64",
                "video5SecondViewRate": "FLOAT64",
                "newToBrandUnitsSoldClicks": "INT64",
                "newToBrandUnitsSoldPercentage": "FLOAT64",
                "campaignName": "STRING",
            }
        },
        Constant.ADS_PURCHASED_PRODUCT: {
            'partitionColumn': 'date',
            'loadMethod': 'merge',
            'mergeColumns': ['marketplace', 'profileId', 'date', 'campaignId', 'adGroupId', 'purchasedAsin'],
            'dataset': 'ads_sb',
            'tableName': 'reports_purchased_product',
            'schema': {
                "marketplace": "STRING",
                "profileId": "STRING",
                "date": "TIMESTAMP",
                "last_updated": "TIMESTAMP",
        
                "purchasedAsin": "STRING",
                "adGroupId": "INT64",
                "adGroupName": "STRING",
                "campaignId": "INT64",
                "campaignName": "STRING",
                "campaignBudgetCurrencyCode": "STRING",
                "campaignPriceTypeCode": "STRING",
                "productName": "STRING",
                "productCategory": "STRING",
                "attributionType": "STRING",
        
                "unitsSold14d": "INT64",
                "unitsSoldClicks14d": "INT64",
                "sales14d": "FLOAT64",
                "salesClicks14d": "FLOAT64",
                "newToBrandSales14d": "FLOAT64",
                "newToBrandUnitsSold14d": "INT64",
                "newToBrandUnitsSoldPercentage14d": "FLOAT64",
                "newToBrandPurchasesPercentage14d": "FLOAT64",
                "newToBrandSalesPercentage14d": "FLOAT64",
                "newToBrandPurchases14d": "FLOAT64",
                "orders14d": "INT64",
                "ordersClicks14d": "INT64",
            }
        },
        Constant.ADS_TARGET: {
            'partitionColumn': 'date',
            'loadMethod': 'merge',
            'mergeColumns': ['marketplace', 'profileId', 'date', 'keywordId', 'targetingId', 'adGroupId', 'campaignId'],
            'dataset': 'ads_sb',
            'tableName': 'reports_target',
            'schema': {
                "marketplace": "STRING",
                "profileId": "STRING",
                "date": "TIMESTAMP",
                "last_updated": "TIMESTAMP",
        
                "keywordId": "INT64",
                "keywordText": "STRING",
                "keywordType": "STRING",
                "keywordBid": "FLOAT64",
                "matchType": "STRING",
                "adKeywordStatus": "STRING",
                "targetingId": "INT64",
                "targetingText": "STRING",
                "targetingExpression": "STRING",
                "targetingType": "STRING",
                "costType": "STRING",
        
                "campaignId": "INT64",
                "campaignName": "STRING",
                "campaignStatus": "STRING",
                "campaignBudgetType": "STRING",
                "campaignBudgetAmount": "FLOAT64",
                "campaignBudgetCurrencyCode": "STRING",
        
                "adGroupId": "INT64",
                "adGroupName": "STRING",
        
                "qualifiedBorrows": "INT64",
                "topOfSearchImpressionShare": "FLOAT64",
                "royaltyQualifiedBorrows": "INT64",
                "addToList": "INT64",
                "cost": "FLOAT64",
                "impressions": "INT64",
                "clicks": "INT64",
        
                "newToBrandSalesClicks": "INT64",
                "purchasesClicks": "INT64",
                "detailPageViews": "INT64",
                "newToBrandSalesPercentage": "FLOAT64",
                "unitsSold": "INT64",
                "sales": "FLOAT64",
                "qualifiedBorrowsFromClicks": "INT64",
                "addToListFromClicks": "INT64",
        
                "royaltyQualifiedBorrowsFromClicks": "INT64",
                "purchasesPromoted": "INT64",
                "newToBrandDetailPageViewRate": "FLOAT64",
                "purchases": "INT64",
                "newToBrandPurchasesPercentage": "FLOAT64",
        
                "salesPromoted": "FLOAT64",
                "salesClicks": "FLOAT64",
                "newToBrandPurchasesRate": "FLOAT64",
                "brandedSearches": "INT64",
                "newToBrandECPDetailPageView": "FLOAT64",
                "newToBrandPurchases": "INT64",
                "brandedSearchesClicks": "INT64",
                "newToBrandSales": "FLOAT64",
                "newToBrandPurchasesClicks": "INT64",
                "addToCartClicks": "INT64",
                "addToCart": "INT64",
                "addToCartRate": "FLOAT64",
                "newToBrandDetailPageViewsClicks": "INT64",
                "newToBrandUnitsSold": "INT64",
        
                "detailPageViewsClicks": "INT64",
                "eCPAddToCart": "FLOAT64",
                "newToBrandDetailPageViews": "INT64",
                "newToBrandUnitsSoldClicks": "INT64",
                "newToBrandUnitsSoldPercentage": "FLOAT64"
            }
        },
    },
    Constant.ADTYPE_SD: {
        Constant.ADS_CAMPAIGN: {
            'partitionColumn': 'date',
            'loadMethod': 'merge',
            'mergeColumns': ['marketplace', 'profileId', 'date', 'campaignId'],
            'dataset': 'ads_sd',
            'tableName': 'reports_campaign',
            'schema': {
                "marketplace": "STRING",
                "profileId": "STRING",
                "date": "TIMESTAMP",
                "last_updated": "TIMESTAMP",
        
                "addToCart": "FLOAT64",
                "addToCartClicks": "INT64",
                "addToCartRate": "FLOAT64",
                "addToCartViews": "INT64",
                "brandedSearches": "INT64",
                "brandedSearchesClicks": "INT64",
                "brandedSearchesViews": "INT64",
                "brandedSearchRate": "FLOAT64",
                "campaignBudgetCurrencyCode": "STRING",
                "campaignId": "INT64",
                "campaignName": "STRING",
                "clicks": "INT64",
                "cost": "FLOAT64",
                "detailPageViews": "INT64",
                "detailPageViewsClicks": "INT64",
                "eCPAddToCart": "FLOAT64",
                "eCPBrandSearch": "FLOAT64",
                "impressions": "INT64",
                "impressionsViews": "INT64",
                "newToBrandPurchases": "INT64",
                "newToBrandPurchasesClicks": "INT64",
                "newToBrandSalesClicks": "INT64",
                "newToBrandUnitsSold": "INT64",
                "newToBrandUnitsSoldClicks": "INT64",
                "purchases": "INT64",
                "purchasesClicks": "INT64",
                "purchasesPromotedClicks": "INT64",
                "sales": "FLOAT64",
                "salesClicks": "INT64",
                "salesPromotedClicks": "INT64",
                "unitsSold": "INT64",
                "unitsSoldClicks": "INT64",
                "videoCompleteViews": "INT64",
                "videoFirstQuartileViews": "INT64",
                "videoMidpointViews": "INT64",
                "videoThirdQuartileViews": "INT64",
                "videoUnmutes": "INT64",
                "viewabilityRate": "FLOAT64",
                "viewClickThroughRate": "FLOAT64",
                "campaignBudgetAmount": "FLOAT64",
                "campaignStatus": "STRING",
                "costType": "STRING",
                "cumulativeReach": "FLOAT64",
                "impressionsFrequencyAverage": "FLOAT64",
                "newToBrandDetailPageViewClicks": "INT64",
                "newToBrandDetailPageViewRate": "FLOAT64",
                "newToBrandDetailPageViews": "INT64",
                "newToBrandDetailPageViewViews": "INT64",
                "newToBrandECPDetailPageView": "INT64",
                "newToBrandSales": "FLOAT64",
            }
        },
        Constant.ADS_ADVERTISED_PRODUCT: {
            'partitionColumn': 'date',
            'loadMethod': 'merge',
            'mergeColumns': ['marketplace', 'profileId', 'date', 'campaignId', 'adGroupId', 'adId'],
            'dataset': 'ads_sd',
            'tableName': 'reports_advertised_product',
            'schema': {
                "marketplace": "STRING",
                "profileId": "STRING",
                "date": "TIMESTAMP",
                "last_updated": "TIMESTAMP",
        
                "addToCart": "FLOAT64",
                "addToCartClicks": "FLOAT64",
                "addToCartRate": "FLOAT64",
                "addToCartViews": "FLOAT64",
                "adGroupId": "INT64",
                "adGroupName": "STRING",
                "adId": "INT64",
                "bidOptimization": "STRING",
                "brandedSearches": "FLOAT64",
                "brandedSearchesClicks": "FLOAT64",
                "brandedSearchesViews": "FLOAT64",
                "brandedSearchRate": "FLOAT64",
                "campaignBudgetCurrencyCode": "STRING",
                "campaignId": "INT64",
                "campaignName": "STRING",
                "clicks": "INT64",
                "cost": "FLOAT64",
                "cumulativeReach": "FLOAT64",
                "detailPageViews": "FLOAT64",
                "detailPageViewsClicks": "FLOAT64",
                "eCPAddToCart": "FLOAT64",
                "eCPBrandSearch": "FLOAT64",
                "impressions": "INT64",
                "impressionsFrequencyAverage": "FLOAT64",
                "impressionsViews": "FLOAT64",
                "newToBrandDetailPageViewClicks": "FLOAT64",
                "newToBrandDetailPageViewRate": "FLOAT64",
                "newToBrandDetailPageViews": "FLOAT64",
                "newToBrandDetailPageViewViews": "FLOAT64",
                "newToBrandECPDetailPageView": "FLOAT64",
                "newToBrandPurchases": "FLOAT64",
                "newToBrandPurchasesClicks": "FLOAT64",
                "newToBrandSales": "FLOAT64",
                "newToBrandSalesClicks": "FLOAT64",
                "newToBrandUnitsSold": "FLOAT64",
                "newToBrandUnitsSoldClicks": "FLOAT64",
                "promotedAsin": "STRING",
                "promotedSku": "STRING",
                "purchases": "FLOAT64",
                "purchasesClicks": "FLOAT64",
                "purchasesPromotedClicks": "FLOAT64",
                "sales": "FLOAT64",
                "salesClicks": "FLOAT64",
                "salesPromotedClicks": "FLOAT64",
                "unitsSold": "INT64",
                "unitsSoldClicks": "FLOAT64",
                "videoCompleteViews": "FLOAT64",
                "videoFirstQuartileViews": "FLOAT64",
                "videoMidpointViews": "FLOAT64",
                "videoThirdQuartileViews": "FLOAT64",
                "videoUnmutes": "FLOAT64",
                "viewabilityRate": "FLOAT64",
                "viewClickThroughRate": "FLOAT64"
            }
        },
        Constant.ADS_PURCHASED_PRODUCT: {
            'partitionColumn': 'date',
            'loadMethod': 'merge',
            'mergeColumns': ['marketplace', 'profileId', 'date', 'campaignId', 'adGroupId', 'promotedAsin',
                             'asinBrandHalo', 'promotedSku'],
            'dataset': 'ads_sd',
            'tableName': 'reports_purchased_product',
            'schema': {
                "marketplace": "STRING",
                "profileId": "STRING",
                "date": "TIMESTAMP",
                "last_updated": "TIMESTAMP",
        
                "promotedAsin": "STRING",
                "asinBrandHalo": "STRING",
                "promotedSku": "STRING",
                "adGroupId": "INT64",
                "adGroupName": "STRING",
                "campaignId": "INT64",
                "campaignName": "STRING",
                "campaignBudgetCurrencyCode": "STRING",
        
                "royaltyQualifiedBorrows": "INT64",
                "addToList": "FLOAT64",
                "qualifiedBorrows": "INT64",
                "unitsSoldBrandHaloClicks": "INT64",
                "royaltyQualifiedBorrowsFromViews": "INT64",
                "addToListFromViews": "INT64",
                "salesBrandHalo": "FLOAT64",
                "qualifiedBorrowsFromClicks": "INT64",
                "qualifiedBorrowsFromViews": "INT64",
                "conversionsBrandHaloClicks": "FLOAT64",
                "addToListFromClicks": "INT64",
                "unitsSoldBrandHalo": "INT64",
                "salesBrandHaloClicks": "FLOAT64",
                "conversionsBrandHalo": "FLOAT64",
                "royaltyQualifiedBorrowsFromClicks": "INT64",
            }
        },
        Constant.ADS_TARGET: {
            'partitionColumn': 'date',
            'loadMethod': 'merge',
            'mergeColumns': ['marketplace', 'profileId', 'date', 'targetingId', 'adGroupId', 'campaignId'],
            'dataset': 'ads_sd',
            'tableName': 'reports_target',
            'schema': {
                "marketplace": "STRING",
                "profileId": "STRING",
                "date": "TIMESTAMP",
                "last_updated": "TIMESTAMP",
        
                "adKeywordStatus": "STRING",
                "targetingId": "INT64",
                "targetingText": "STRING",
                "targetingExpression": "STRING",
        
                "campaignId": "INT64",
                "campaignName": "STRING",
                "campaignBudgetCurrencyCode": "STRING",
        
                "adGroupId": "INT64",
                "adGroupName": "STRING",
        
                "qualifiedBorrows": "INT64",
                "royaltyQualifiedBorrows": "INT64",
                "addToList": "INT64",
                "cost": "FLOAT64",
                "impressions": "INT64",
                "clicks": "INT64",
                "newToBrandSalesClicks": "INT64",
                "purchasesClicks": "INT64",
                "detailPageViews": "INT64",
                "unitsSold": "INT64",
                "sales": "FLOAT64",
                "qualifiedBorrowsFromClicks": "INT64",
                "addToListFromClicks": "INT64",
                "royaltyQualifiedBorrowsFromClicks": "INT64",
                "newToBrandDetailPageViewRate": "FLOAT64",
                "purchases": "INT64",
                "salesClicks": "FLOAT64",
                "brandedSearches": "INT64",
                "newToBrandECPDetailPageView": "FLOAT64",
                "newToBrandPurchases": "INT64",
                "brandedSearchesClicks": "INT64",
                "newToBrandSales": "FLOAT64",
                "newToBrandPurchasesClicks": "INT64",
                "addToCartClicks": "INT64",
                "addToCart": "INT64",
                "addToCartRate": "FLOAT64",
                "newToBrandUnitsSold": "INT64",
                "detailPageViewsClicks": "INT64",
                "eCPAddToCart": "FLOAT64",
                "newToBrandDetailPageViews": "INT64",
                "newToBrandUnitsSoldClicks": "INT64",
                "viewabilityRate": "FLOAT64",
                "addToListFromViews": "INT64",
                "addToCartViews": "INT64",
                "qualifiedBorrowsFromViews": "INT64",
                "newToBrandDetailPageViewClicks": "INT64",
                "viewClickThroughRate": "FLOAT64",
                "royaltyQualifiedBorrowsFromViews": "INT64",
                "newToBrandDetailPageViewViews": "INT64",
                "leadFormOpens": "INT64",
                "leads": "INT64",
                "videoCompleteViews": "INT64",
                "impressionsViews": "INT64",
                "videoFirstQuartileViews": "INT64",
                "unitsSoldClicks": "INT64",
                "brandedSearchRate": "FLOAT64",
                "linkOuts": "INT64",
                "brandedSearchesViews": "INT64",
                "videoUnmutes": "INT64",
                "videoThirdQuartileViews": "INT64",
                "salesPromotedClicks": "INT64",
                "eCPBrandSearch": "FLOAT64",
                "purchasesPromotedClicks": "INT64",
                "videoMidpointViews": "INT64",
            }
        },
    },
    Constant.ADTYPE_DSP: {

    },
    Constant.ADTYPE_ST: {

    }
}


def normalize_column_name(column):
    column = column.replace(" (", "(")
    column = column.replace("( ", "(")
    column = column.replace(" )", ")")
    column = column.replace(") ", ")")
    if column.endswith(")"):
        column = column[:-1]

    column = re.sub(r'[\s\-()\[\]{}]', '_', column)
    column = re.sub(r'[,./]', '', column)

    return column


def df_columns_new_name_mapping(columns):
    renamed_columns = {}
    for column in columns:
        renamed_columns[column] = normalize_column_name(column)
    return renamed_columns


class TableConfig:
    def __init__(self, table_type, ad_type, table_name):
        self.ad_type = ad_type
        self.table_name = table_name
        self.table_type = table_type

    @staticmethod
    def getTablesConfig(table_type):
        if table_type == Constant.TBL_TYPE_GENERAL:
            return GENERAL_TABLES
        elif table_type == Constant.TBL_TYPE_REPORT:
            return REPORTS
        elif table_type == Constant.TBL_TYPE_ENTITY:
            return ENTITIES
        else:
            return []

    @staticmethod
    def getTableNames(table_type, ad_type, givenTables=None):
        tablesConfig = TableConfig.getTablesConfig(table_type)
        tables = tablesConfig.get(ad_type, {}).keys()

        givenTables = [] if givenTables is None else givenTables

        if isinstance(givenTables, str):
            givenTables = givenTables.split(',')

        givenTables = [element for element in givenTables if
                       element or element is False or element == 0 or element != '']

        if len(givenTables) > 0:
            tables = [a for a in tables if a in givenTables]

        return tables

    @staticmethod
    def getAdTypes(table_type, givenAdTypes=None):
        tablesConfig = TableConfig.getTablesConfig(table_type)
        givenAdTypes = [] if givenAdTypes is None else givenAdTypes

        givenAdTypes = [element for element in givenAdTypes if
                        element or element is False or element == 0 or element != '']

        adTypes = tablesConfig.keys()
        if len(givenAdTypes) > 0:
            adTypes = [a for a in adTypes if a in givenAdTypes]
        return adTypes

    def getTableAttr(self, attr, defaultValue=None):
        tablesConfig = TableConfig.getTablesConfig(self.table_type)
        config = tablesConfig.get(self.ad_type, None)
        if config is not None:
            config = config.get(self.table_name, None)
            if config is not None:
                config = config.get(attr, defaultValue)
        return config if config is not None else defaultValue

    def getSchema(self):
        schema = self.getTableAttr('schema')
        final_schema = {}
        for column, dataType in schema.items():
            final_schema[normalize_column_name(column)] = dataType
        return final_schema

    def getDateFormats(self):
        return self.getTableAttr('dateFormats', {})

    def getTableName(self):
        return self.getTableAttr('tableName')

    def getDataset(self):
        dataset = self.getTableAttr('dataset')
        return "staging_{}".format(dataset) if os.getenv("USE_STAGING_BQ", "no") == 'yes' else dataset

    def getMergeColumns(self):
        return self.getTableAttr('mergeColumns', [])

    def getLoadMethod(self):
        return self.getTableAttr('loadMethod', 'merge')

    def getPartitionColumn(self):
        return self.getTableAttr('partitionColumn', None)

    def getClusterColumn(self):
        return self.getTableAttr('clusterColumn', None)
