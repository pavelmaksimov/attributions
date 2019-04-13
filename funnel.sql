SELECT
    CounterID, ClientID, PurchaseID, DateTimePurchase, Revenue, VisitIDPurchase,
    DateTimeVisits,
    VisitIDsSources,
    Sources,
    CoefsNorm,
    toUInt32(Revenue)  * CoefsNorm as AttributionRevenue
FROM(
    SELECT
        CounterID, ClientID, PurchaseID, DateTimePurchase, Revenue, VisitIDPurchase,
        groupArray(DateTimeVisit) as DateTimeVisits_,
        groupArray(VisitIDSource) as VisitIDsSources_,
        groupArray(Source) as Sources_,
        groupArray(Coef) as Coefs,
        arraySum(Coefs) as CoefsSum,
        arrayMap(x -> x / CoefsSum, Coefs) as CoefsNorm
    FROM(
        SELECT
            CounterID, ClientID, PurchaseID, VisitIDPurchase, DateTimePurchase, Revenue,
            DateTimeVisit, VisitIDSource, Source, Coef
        FROM(
            SELECT
                CounterID, ClientID, PurchaseID, VisitIDPurchase, DateTimePurchase, Revenue,
                groupArray(DateTimeVisit) as DateTimeVisits,
                groupArray(VisitIDSource) as VisitIDSources,
                groupArray(Source) as Sources,
                groupArray(Goal) as Goals,
                Goal,
                length(Goals) as EveryGoalCount,
                transform(Goal, ['product', 'product_purchase', 'basket', 'purchase'], [0.1, 0.5, 0.3, 0.1], 0.0) as Weight,
                Weight / EveryGoalCount as Coef
            FROM(
                SELECT 
                    CounterID, ClientID, PurchaseID, DateTimePurchase, Revenue, VisitIDPurchase, 
                    VisitIDSource, VisitIDTarget, DateTimeVisit_ as DateTimeVisit, Source, Goal
                FROM(
                    SELECT
                        CounterID, ClientID, PurchaseID, DateTimePurchase, Revenue, VisitIDPurchase, VisitIDTarget,
                        argMin(DateTimeVisit, DateTimeVisit) as DateTimeVisit_,
                        argMin(VisitIDSource, DateTimeVisit) as VisitIDSource,
                        argMin(Source, DateTimeVisit) as Source,
                        groupArrayIf(Goal, Goal IN ('2page', 'basket', 'product_purchase', 'purchase')) as Goals
                    FROM(
                        SELECT 
                            CounterID, ClientID, PurchaseID, DateTimePurchase, Revenue, VisitIDPurchase,
                            DateTimeVisit, VisitIDSource, Source, Goal, VisitIDTarget, ProductImpressions, ProductPurchases
                        FROM(
                            SELECT
                                CounterID, ClientID, PurchaseID, DateTimePurchase, Revenue, VisitIDPurchase,
                                DateTimeVisit, VisitIDSource, Source, VisitIDTarget, ProductImpressions, ProductPurchases,
                                arrayConcat(
                                    Goals_, 
                                    [
                                        if(HasGoalImpressionProductPurchase, 'product_purchase', ''),
                                        if(VisitIDPurchase = VisitIDSource, 'purchase', '')
                                    ]
                                ) as Goals
                            FROM(
                                SELECT
                                    *
                                FROM(
                                    SELECT
                                        CounterID, ClientID, PurchaseID, DateTimePurchase, Revenue, VisitIDPurchase, ProductPurchases,
                                        groupArray(DateTimeVisit) as DateTimeVisits,
                                        groupArray(VisitIDSource) as VisitIDSources,
                                        groupArray(ProductImpressions) as ProductImpressionss,
                                        groupArray(Source) as Sources,
                                        groupArray(Goals) as Goalss,
                                        groupArray(HasGoalImpressionProductPurchase) as HasGoalImpressionProductPurchases,
                                        groupArray(Except) as Except,
                                        groupArray(ExceptNotSignSource) as ExceptNotSignSource,
                                        arrayCount(Except) = arraySum(Except) as NotSignSource,
                                        if(
                                            NotSignSource,
                                            arrayReverseSort(groupArrayIf(NoExceptNotSignSource, NoExceptNotSignSource != 0)),
                                            arrayReverseSort(groupArrayIf(NoExcept, NoExcept != 0))
                                        ) as NoExceptSelected,
                                        if(
                                            NotSignSource,
                                            arrayMap(x,y -> if(y=1, arrayFirst(n -> x>n, NoExceptSelected), x), VisitIDSources, ExceptNotSignSource),
                                            arrayMap(x,y -> if(y=1, arrayFirst(n -> x>n, NoExceptSelected), x), VisitIDSources, Except)
                                        ) as VisitIDTargets
                                    FROM(
                                        SELECT
                                            if(Source IN ('прямой', 'брендовый контекст'), 1, 0) as Except,
                                            if(Source IN ('прямой'), 1, 0) as ExceptNotSignSource,
                                            if(Except = 0, VisitIDSource, 0) as NoExcept,
                                            if(ExceptNotSignSource = 0, VisitIDSource, 0) as NoExceptNotSignSource,
                                            hasAny(ProductPurchases, ProductImpressions) as HasGoalImpressionProductPurchase,
                                            *
                                        FROM(
                                            SELECT 
                                                toUInt16(CounterID) as CounterID,
                                                toUInt16(ClientID) as ClientID,
                                                argMin(toUInt8(VisitID), toDateTime(DateTime)) as VisitIDPurchase, 
                                                argMin(toDateTime(DateTime), toDateTime(DateTime)) as DateTimePurchase,
                                                splitByChar(',', Revenue) as Revenue,
                                                splitByChar(',', PurchaseID) as PurchaseID, 
                                                splitByChar(',', ProductPurchases) as ProductPurchases
                                            FROM 
                                                test.new_model_attribution2
                                            WHERE 
                                                PurchaseID != ['']
                                            GROUP BY 
                                                ClientID, CounterID, PurchaseID, Revenue, ProductPurchases
                                        )
                                        ALL LEFT JOIN(
                                            SELECT
                                                toUInt16(CounterID) as CounterID, 
                                                toUInt8(VisitID) as VisitIDSource,
                                                toUInt16(ClientID) as ClientID,
                                                toDateTime(DateTime) as DateTimeVisit,
                                                Source,
                                                splitByChar(',', Goals) as Goals,
                                                splitByChar(',', ProductImpressions) as ProductImpressions
                                            FROM 
                                                test.new_model_attribution2
                                            )
                                        USING 
                                            CounterID, ClientID
                                        WHERE 
                                            DateTimeVisit <= DateTimePurchase -- исключение визитов, после транзакции
                                            AND toDate(DateTimeVisit) >= toDate(DateTimePurchase) - 90 -- фильтрация визитов в рамках ретроокна
                                        ORDER BY 
                                            PurchaseID, DateTimeVisit
                                    )
                                    GROUP BY 
                                        CounterID, ClientID, PurchaseID, DateTimePurchase, Revenue, VisitIDPurchase, ProductPurchases
                                )
                            )
                            ARRAY JOIN 
                                DateTimeVisits as DateTimeVisit,
                                VisitIDSources as VisitIDSource,
                                Sources as Source,
                                Goalss as Goals_,
                                VisitIDTargets as VisitIDTarget,
                                ProductImpressionss as ProductImpressions,
                                HasGoalImpressionProductPurchases as HasGoalImpressionProductPurchase
                            ORDER BY 
                                PurchaseID, DateTimeVisit
                        )
                        ARRAY JOIN Goals as Goal
                    )
                    GROUP BY 
                        CounterID, ClientID, PurchaseID, DateTimePurchase, Revenue, VisitIDPurchase, VisitIDTarget
                    ORDER BY 
                        PurchaseID, DateTimeVisit_
                )
                ARRAY JOIN 
                    Goals as Goal
            )
            Array JOIN 
                Revenue as Revenue,
                PurchaseID as PurchaseID
            GROUP BY 
                CounterID, ClientID, PurchaseID, DateTimePurchase, Revenue, VisitIDPurchase, Goal
            ORDER BY PurchaseID, VisitIDPurchase
        )
        Array JOIN 
            DateTimeVisits as DateTimeVisit,
            VisitIDSources as VisitIDSource,
            Sources as Source
    )
    GROUP BY 
        CounterID, ClientID, PurchaseID, DateTimePurchase, Revenue, VisitIDPurchase
)
Array JOIN 
    DateTimeVisits_ as DateTimeVisits,
    VisitIDsSources_ as VisitIDsSources,
    Sources_ as Sources,
    CoefsNorm as CoefsNorm