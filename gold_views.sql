
create or replace view FCO.GOLD.FACTCURRENTSTOCK(
	LOCATIONID,
	ITEMID,
	ITEMVARIANTID,
	UNITPRICE,
	QTYONHAND,
	QTYREMAINING,
	COSTAMOUNTACTUAL,
	COSTAMOUNTEXPECTED,
	COSTAMOUNTTOTAL,
	QTYUNPOSTED,
	QTYONSO,
	QTYONTO,
	QTY_AVAILABLE,
	QTYQC,
	QTY_GITREC,
	COST_GITREC,
	SELLEX_GITREC,
	QTY_PRO,
	QTY_RES,
	QTY_ONORDER,
	COST_ONORDER,
	SELLEX_ONORDER,
	ACTIVEVARIANTUNITPRICE,
	ONMD,
	FIRSTRECDATE,
	LASTRECDATE,
	LASTTRSDATE,
	LASTSOLDDATE,
	DAYSSINCELASTSOLD,
	DAYSSINCELASTTRS,
	WKSEXPOSED,
	WKSSINCELASTREC,
	QTY_SOR,
	SELLEX_SOR,
	COST_SOR,
	HASSTOCK,
	QTYSOLDL8WEEKS,
	QTYRECEIVED,
	QTYSOLD,
	L_RECAGEING,
	L_RECAGEINGSORT,
	L_SOLDAGEING,
	L_SOLDAGEINGSORT,
	L_TROAGEING,
	L_TROAGEINGSORT,
	CURRENT_WEEK,
	WEEK_1_AGO,
	WEEK_2_AGO,
	WEEK_3_AGO,
	WEEK_4_AGO,
	WEEK_5_AGO,
	WEEK_6_AGO,
	WEEK_7_AGO,
	TOTAL_8_WEEKS,
	AVG_WEEKLY_SALES,
	WEEKS_WITH_SALES
) as
            WITH
    UP --Done
    AS
    (

        SELECT TSE.CompanyName,
            DL.LocationCode LocationCode,
            TSE.itemNo  ,
            TSE.variantCode VariantCode,
            SUM(TSE.Quantity) Quantity
       FROM fco.silver.factlsctrans_salesentry TSE  
            LEFT JOIN  fco.silver.factlsctrans_salesentrystatus TSES  
            ON TSES.transactionNo = TSE.transactionNo
                AND TSES.storeNo  = TSE.storeNo
                AND TSES.itemNo   = TSE.itemNo
                AND TSES.lineNo = TSE.lineNo
                AND TSES.variantCode  = TSE.variantCode
                and TSES.CompanyName = TSE.CompanyName
            left join fco.silver.dimlocation SL  
            on SL.CompanyName = TSE.CompanyName
                and SL.code = TSE.storeNo
            left join fco.gold.vdimlocation DL  
            on DL.CompanyName = SL.CompanyName
                and DL.LocationCode = SL.code
        WHERE TSES.status != 'Posted'
            OR TSES.transactionNo IS NULL
        GROUP BY TSE.CompanyName,
        DL.LocationCode,
        TSE.itemNo,
        DL.LocationCode,
        TSE.variantCode
    ),


    TRO --done
as
(
    SELECT a.CompanyName,
        b.transferFromCode LocationFrom,
        b.itemNo AS ItemNo,
        b.variantCode AS Variant,
        SUM(b.outstandingQuantity) AS Qty_TO
FROM fco.silver.factTransferHeader a
        LEFT JOIN fco.silver.factTransferLine b 
        ON a.no = b.documentNo
        and a.CompanyName = b.CompanyName
 where a.Status = 'Released'
    GROUP BY a.CompanyName,
     b.transferFromCode,
     b.itemNo,
     b.variantCode
    HAVING SUM(b.outstandingQuantity) <> 0
),

QC --done
as
(
    SELECT CompanyName,
        itemNo  ,
        variantCode VariantCode,
        lastLocationCode LocationCode,
        SUM(quantity) QCSOH
    FROM fco.silver.factLotNo_Information  
    where qcRequired = 'True'
    GROUP BY CompanyName,
     itemNo,
     variantCode,
     lastLocationCode
),
        SO --Done
        as
        (
            SELECT a.CompanyName,
                a.locationCode LocationCode,
                b.no AS ItemNo,
                b.variantCode AS Variant,
                SUM(   CASE
                      WHEN a.documentType = 'Return Order' THEN
                          b.outstandingQuantity * -1
                      ELSE
                          b.outstandingQuantity
                  END
              ) AS Qty_SO
     FROM fco.silver.factSalesHeader a 
                LEFT JOIN fco.silver.factSalesLine b 
                ON a.no = b.documentNo
            WHERE b.Type = 'Item'
                and a.Status = 'Released'
                AND a.documentType IN ( 'Order', 'Return Order' )
            GROUP BY a.CompanyName,
             a.locationCode,
             b.no,
             b.variantCode
            HAVING SUM(b.outstandingQuantity) <> 0
        ),
        gitRec --done
        as
        (
            SELECT TH.CompanyName,
                COALESCE(TH.transferToCode, TL.transferToCode) ToLocation,
                TL.itemNo  ,
                TL.variantCode VariantCode,
                sum(TL.quantityShipped - TL.quantityReceived) as QtyOut
            FROM fco.silver.factTransferHeader TH  
 
                LEFT JOIN fco.silver.factTransferLine TL  
                ON TL.CompanyName = TH.CompanyName
                    AND TL.documentNo = TH.no
            group by TH.CompanyName,
            COALESCE(TH.transferToCode, TL.transferToCode),
            TL.itemNo, 
            TL.variantCode
        ),
        SOR --done
as
(

    SELECT
        h.CompanyName,
        l.transferToCode ,
        l.itemNo ,
        l.variantCode ,
        CAST(l.Quantity - l.quantityShipped AS INT) SOR

    FROM fco.silver.factTransferHeader h
        LEFT JOIN fco.silver.factTransferLine l
        ON h.CompanyName = l.CompanyName
            AND h.no = l.documentNo

    WHERE h.Status = 'Released' AND
        l.quantity - l.quantityShipped <> 0 AND
        l.derivedFromLineNo = 0

),




        PRO
        as
        (
            select PH.CompanyName,
                PL.locationCode LocationCode,
                PL.no ItemNo,
                Pl.variantCode VariantCode,
                sum(Quantity) QTY_PRO
            from fco.silver.factPurchaseHeader PH
                left join fco.silver.factPurchaseLine PL
                on PL.documentNo = PH.no
                and PL.CompanyName = PH.companyName
            where PH.documentType in  ('Return Order' )
                and PH.Status = 'Released'
            group by PH.CompanyName,
             PL.locationCode,
             PL.no,
             Pl.variantCode
            HAVING sum(quantity) <> 0
        ),

        
        RES --done
        as
        (
  SELECT CompanyName,
                locationCode ,
                itemNo ItemNo,
                variantCode ,
                CASE
               WHEN SUM(quantity) < 0 THEN
                   SUM(quantity) * -1
               ELSE
                   SUM(quantity)
           END QTY_RES
            FROM fco.silver.factReservationEntry RE
            WHERE Positive = 'False'
            and sourceID not in (select no from fco.silver.factTransferHeader where Status = 'Released')
            and sourceID not in (select no from fco.silver.factSalesHeader where Status = 'Released')
            GROUP BY CompanyName,
             locationCode,
             itemNo,
             variantCode
            HAVING sum(quantity) <> 0
        ),
        PRICV --done
        as
        (
            SELECT SPI.CompanyName,
                SPI.itemNo  ,
                SPI.variantCode  ,
                SPI.startingDate  ,
                SPI.endingDate ,
                SPI.unitPrice  ,
                SPI.onMarkdown OnMD,
                ROW_NUMBER() OVER (PARTITION BY SPI.CompanyName,
                                           SPI.itemNo,
                                           SPI.variantCode
                              ORDER BY SPI.CompanyName,
                                       SPI.itemNo,
                                       SPI.variantCode,
                                       SPI.startingDate DESC
                             ) AS RN
            from fco.silver.factSalesPrice SPI 
              
            where try_to_date( SPI.startingDate ) < getdate()
                AND SPI.salesCode = 'ALL'
                AND (
                  try_to_date( SPI.startingDate ) > GETDATE()
                OR try_to_date( SPI.startingDate ) = '0001-01-01'
              )
            --  and [Unit of Measure Code] = 'EACH'  
        ),
        PRIC --done
        as
        (
             SELECT SPI.CompanyName,
                SPI.itemNo  ,
                SPI.startingDate  ,
                SPI.endingDate  ,
                SPI.unitPrice  ,
                SPI.onMarkdown OnMD ,
                ROW_NUMBER() OVER (PARTITION BY SPI.CompanyName,
                                           SPI.itemNo
                              ORDER BY SPI.CompanyName,
                                       SPI.itemNo,
                                       try_to_date( SPI.startingDate ) DESC
                             ) AS RN
            from fco.silver.factSalesPrice SPI  
            where try_to_date( SPI.startingDate ) < getdate()
                AND SPI.salesCode = 'ALL'
                AND (
                  try_to_date(SPI.endingDate) > GETDATE()
                OR try_to_date( SPI.endingDate) = '0001-01-01'
              )
                -- and [Unit of Measure Code] = 'EACH'
                and SPI.variantCode is null
        ),
        ONPO --Done
        as
        (
            select PH.CompanyName,
                PL.locationCode  ,
                PL.no ItemNo,
                PL.variantCode  ,
                sum(PL.outstandingQuantity) outstandingQuantity ,
                sum(unitCostLCY * outstandingQuantity) OutstandingCost,
                sum(PL.unitPriceLCY * outstandingQuantity) OutstandingRspIncl
            FROM fco.silver.factPurchaseHeader PH  
              
                LEFT JOIN fco.silver.factPurchaseLine PL  
                ON PL.CompanyName = PH.CompanyName
                    AND PL.documentNo = PH.no
                    AND PL.documentType = PH.documentType
            where PH.documentType = 'Order'
                and outstandingQuantity > 0
                and status = 'Released'
         
                and lscRetailStatus in ('New',
'Part. receipt',
'Sent')
            group by PH.CompanyName,
             PL.no,
             PL.variantCode,
             PL.locationCode
        )

        ,
 
     frd 
     as
     (
         select CompanyName,
             locationCode  ,
             itemNo  ,
             variantCode  ,
             min(try_to_date(postingDate) ) as FirstRecDate
         from fco.silver.factItemLedgerEntry LE  
         where Quantity > 0
         group by CompanyName,
          CompanyName,
          locationCode,
          itemNo,
          variantCode
     )  ,
     lrd
        as
        (
            select CompanyName,
                locationCode  ,
                itemNo ,
                variantCode  ,
                 max(try_to_date(postingDate)) as LastRecDate
            from fco.silver.factItemLedgerEntry LE
            where  (LE.entryType = 'Purchase' and documentType = ' ') or (LE.entryType = 'Transfer' and documentType = 'Transfer_x0020_Receipt')
            or (LE.entryType = 'Positive_x0020_Adjmt_x002E_' )
            group by CompanyName,
             CompanyName,
             locationCode,
             itemNo,
             variantCode
        ),
        lsd
        as
        (
            select CompanyName,
                locationCode  ,
                itemNo  ,
                variantCode  ,
                 max(try_to_date(postingDate)) as LastSoldDate
            from fco.silver.factItemLedgerEntry LE
            where  (LE.entryType = 'Sale' and documentType in ('_x0020_','Sales_x0020_Shipment')  ) 
            group by CompanyName,
             CompanyName,
             locationCode,
             itemNo,
             variantCode
        ),
        ltrod
        as
        (
            select CompanyName,
                locationCode  ,
                itemNo  ,
                variantCode  ,
                 max(try_to_date(postingDate)) as LastTRSDate
            from fco.silver.factItemLedgerEntry LE
            where  (LE.entryType = 'Transfer' and documentType = 'Transfer_x0020_Shipment')  
            group by CompanyName,
             CompanyName,
             locationCode,
             itemNo,
             variantCode
        ),
--done
ILES as (

    select CompanyName,
                locationCode   ,
                itemNo  ,
                variantCode   ,
                sum(case when entryType = 'Purchase' or (entryType = 'Transfer' and documentType = 'Transfer_x0020_Receipt') 
                or (entryType = 'Positive_x0020_Adjmt_x002E_' and documentType = ' ') then  Quantity  else null end ) as QtyReceived,
                sum(case when entryType = 'Sale' then  Quantity * -1  else null end ) as QtySold
           from fco.silver.factItemLedgerEntry LE
        group by  
        CompanyName,
                locationCode,
                itemNo,
                variantCode 

),
        SL8Wks
        as
        (

        --done
                                        SELECT H.CompanyName,
                    H.ItemNo,
                    H.VariantCode,
                    SUM(H.Quantity) * - 1 Quantity,
                    H.LocationCode
           FROM fco.gold.vFactSalesCreditMemo H  
                  --  LEFT JOIN [saas].[vDimSalesPersonPurchaser] SP
                  --  ON H.[Salesperson Code] = SP.[Salesperson Code] AND H.CompanyName = SP.CompanyName
                 --   left join vDimSalesStaff SS on SS.CompanyName = H.CompanyName
                  --      and SS.[Sales Staff ID] = H.[Salesperson Code]

                WHERE H.LocationCode <> ''
                    AND H.Type = 'Item'
                    AND H.Quantity != 0
                    and PostingDate > DATEADD(DD,-56,GETDATE())
                GROUP BY 
              H.CompanyName,H.VariantCode, 
             H.ItemNo, 
             H.LocationCode
            union all
            --done
         --       SELECT H.CompanyName,
         --           H.[Item No_] [Item No_],
         --           H.[Variant Code],
        --            SUM(H.Quantity) Quantity,
        --            H.[Location Code]
        --        FROM saas.vFactSalesInvoice H WITH (NOLOCK)
        --            LEFT JOIN [saas].[vDimSalesPersonPurchaser] SP
        --            ON H.[Salesperson Code] = SP.[Salesperson Code] AND H.CompanyName = SP.CompanyName
        --            left join vDimSalesStaff SS on SS.CompanyName = H.CompanyName
        --                and SS.[Sales Staff ID] = H.[Salesperson Code]

        --        WHERE H.[Location Code] <> ''
        --            AND H.Type = 'Item'
        --            AND H.Quantity != 0
        --            and H.[Posting Date] >= convert(Date,DATEADD(DD,-56,getdate()))

        --        GROUP BY
        --    H.CompanyName, H.[Variant Code],
        --     H.[Item No_],
        --     H.[Location Code]

        --    union all
            --done
                SELECT LSE.CompanyName, itemNo , variantCode , sum(Quantity * -1), DL.LocationCode
                from fco.silver.factLSCTrans_SalesEntry LSE  
                    left join fco.gold.vdimstore DS on DS.StoreNo = LSE.storeNo  
                    and DS.CompanyName = LSE.CompanyName  
                    left join fco.gold.vdimlocation DL on DL.StoreID = DS.StoreSKey 
                where try_to_date(LSE.Date) >= DATEADD(DD,-56,getdate())
                group by LSE.CompanyName,itemNo, LSE.variantCode,  DL.LocationCode

  

        ),
        --done
        Sls8WkSum
        as
        (
            select CompanyName, ItemNo, VariantCode, LocationCode, sum(Quantity) QtySoldL8Weeks
            from SL8Wks
            group by  CompanyName, ItemNo, VariantCode, LocationCode

        )    ,
    --done


 WeeklySales AS (
    SELECT 
        LSE.CompanyName,
        LSE.itemNo,
        LSE.variantCode,
        DL.LocationCode,
        -- Calculate week starting date (Sunday)
        DATE_TRUNC('WEEK', try_to_date(LSE.Date)) AS WeekStartDate,
        -- Week number for reference
        WEEK(try_to_date(LSE.Date)) AS WeekNumber,
        YEAR(try_to_date(LSE.Date)) AS Year,
        -- Friendly week label for column naming
        CONCAT('Week_', TO_CHAR(DATE_TRUNC('WEEK', try_to_date(LSE.Date)), 'MM_DD_YYYY')) AS WeekLabel,
        -- Sales quantity (assuming negative values need to be made positive)
        SUM(LSE.Quantity * -1) AS WeeklySalesQuantity
    FROM fco.silver.factLSCTrans_SalesEntry LSE  
        LEFT JOIN fco.gold.vdimstore DS 
            ON DS.StoreNo = LSE.storeNo  
            AND DS.CompanyName = LSE.CompanyName  
        LEFT JOIN fco.gold.vdimlocation DL 
            ON DL.StoreID = DS.StoreSKey 
    WHERE 
        -- Filter for last 8 weeks (Sunday to Saturday)
        LSE.Date >= DATE_TRUNC('WEEK', DATEADD('WEEK', -8, CURRENT_DATE()))
        AND LSE.Date < DATE_TRUNC('WEEK', DATEADD('WEEK', 1, CURRENT_DATE()))
        -- Add any additional filters you need
        -- AND LSE.CompanyName = 'YourCompanyName'
        -- AND DL.LocationCode IN ('LOC1', 'LOC2')
    GROUP BY 
        LSE.CompanyName,
        LSE.itemNo, 
        LSE.variantCode,  
        DL.LocationCode,
        DATE_TRUNC('WEEK', try_to_date(LSE.Date)),
        WEEK(try_to_date(LSE.Date)),
        YEAR(try_to_date(LSE.Date))
),

WeeklySalesSum
as

(SELECT 
    CompanyName,
    itemNo,
    variantCode,
    LocationCode,
    -- Pivot columns for each of the last 8 weeks (most recent first)
    SUM(CASE WHEN WeekStartDate = DATE_TRUNC('WEEK', CURRENT_DATE()) 
             THEN WeeklySalesQuantity ELSE 0 END) AS Current_Week,
    
    SUM(CASE WHEN WeekStartDate = DATE_TRUNC('WEEK', DATEADD('WEEK', -1, CURRENT_DATE())) 
             THEN WeeklySalesQuantity ELSE 0 END) AS Week_1_Ago,
    
    SUM(CASE WHEN WeekStartDate = DATE_TRUNC('WEEK', DATEADD('WEEK', -2, CURRENT_DATE())) 
             THEN WeeklySalesQuantity ELSE 0 END) AS Week_2_Ago,
    
    SUM(CASE WHEN WeekStartDate = DATE_TRUNC('WEEK', DATEADD('WEEK', -3, CURRENT_DATE())) 
             THEN WeeklySalesQuantity ELSE 0 END) AS Week_3_Ago,
    
    SUM(CASE WHEN WeekStartDate = DATE_TRUNC('WEEK', DATEADD('WEEK', -4, CURRENT_DATE())) 
             THEN WeeklySalesQuantity ELSE 0 END) AS Week_4_Ago,
    
    SUM(CASE WHEN WeekStartDate = DATE_TRUNC('WEEK', DATEADD('WEEK', -5, CURRENT_DATE())) 
             THEN WeeklySalesQuantity ELSE 0 END) AS Week_5_Ago,
    
    SUM(CASE WHEN WeekStartDate = DATE_TRUNC('WEEK', DATEADD('WEEK', -6, CURRENT_DATE())) 
             THEN WeeklySalesQuantity ELSE 0 END) AS Week_6_Ago,
    
    SUM(CASE WHEN WeekStartDate = DATE_TRUNC('WEEK', DATEADD('WEEK', -7, CURRENT_DATE())) 
             THEN WeeklySalesQuantity ELSE 0 END) AS Week_7_Ago,
    
    -- Additional useful columns
    SUM(WeeklySalesQuantity) AS Total_8_Weeks,
    ROUND(AVG(WeeklySalesQuantity), 2) AS Avg_Weekly_Sales,
    COUNT(CASE WHEN WeeklySalesQuantity > 0 THEN 1 END) AS Weeks_With_Sales

FROM WeeklySales
GROUP BY 
    CompanyName,
    itemNo, 
    variantCode,  
    LocationCode
ORDER BY 
    CompanyName,
    LocationCode,
    itemNo,
    variantCode
)



    ,
    LS
    as
    (
        select LE.CompanyName,
            LE.locationCode ,
            LE.itemNo  ,
            LE.variantCode  ,
            SUM(LE.Quantity) QtyOnHand,
            SUM(LE.remainingQuantity) QtyRemaining,
            SUM(VE.CostAmountActual) CostAmountActual,
            SUM(VE.CostAmountExpected) CostAmountExpected,
            SUM(VE.CostAmountExpected + VE.CostAmountActual) CostAmountTotal,
            0 UnitPrice
        from fco.silver.factItemLedgerEntry LE  
            LEFT JOIN fco.gold.vFactValueEntry_ILE VE 
            ON VE.CompanyName = LE.CompanyName
                AND VE.ItemNo = LE.itemNo
                AND VE.LocationCode = LE.locationCode
                AND VE.VariantCode = LE.variantCode
                AND VE.ITEMLEDGERENTRYNO = LE.entryNo
        group by LE.CompanyName,
         LE.locationCode,
         LE.itemNo,
         LE.variantCode
    ) ,
    --new werner all locations x join
    LocSum
    as
    (
                select CompanyName,
                LocationCode,
                ItemNo,
                VariantCode
            from LS
        union ALL
            select CompanyName,
                LocationCode,
                ItemNo,
                VariantCode
            from ONPO
        union all
            select CompanyName,
                LocationFrom,
                ItemNo,
                Variant
            from TRO
        union ALL
            select CompanyName,
                LocationCode,
                ItemNo,
                Variant
            from SO
        union ALL
            select CompanyName,
                LocationCode,
                ItemNo,
                VariantCode
            from QC
        union ALL
            select CompanyName,
                ToLocation,
                ItemNo,
                VariantCode
            from gitRec
        union ALL
            select CompanyName,
                LocationCode,
                ItemNo,
                VariantCode
            from PRO
        union ALL
            select CompanyName,
                LocationCode,
                ItemNo,
                VariantCode
            from RES
        union all
            select CompanyName,
                TransfertoCode,
                ItemNo,
                VariantCode
            from SOR
            union all
            select CompanyName,
                LocationCode,
                ItemNo,
                VariantCode from 
            WeeklySalesSum


    ),
    loc
    as
    (
        select CompanyName,
            LocationCode,
            ItemNo,
            VariantCode Variant
        from LocSum
        group by CompanyName,
         LocationCode,
         ItemNo,
         VariantCode
    )
 
SELECT DL.LocationSKey LocationID,
    I.ItemSKey ItemID,
    IV.ItemVariantSKey ItemVariantID,
    COALESCE(IV.ItemVariantUnitPrice, I.CURRENT_ACTIVE_PRICE_BTR) UnitPrice,
    LS.QtyOnHand,
    LS.QtyRemaining,
    LS.CostAmountActual,
    LS.CostAmountExpected,
    LS.CostAmountTotal,
    --isnull(UP.Quantity,0) QtyUnposted, isnull(S.Qty_SO,0) QtyOnSO, isnull(T.Qty_TO,0) QtyOnTO,
    UP.Quantity QtyUnposted,
    S.Qty_SO QtyOnSO,
    T.Qty_TO QtyOnTO,
     (ifnull(LS.QtyOnHand, 0) - ifnull(QC.QCSOH, 0) - ifnull(T.Qty_TO, 0) - ifnull(S.Qty_SO, 0)
                    + ifnull(UP.Quantity, 0) + ifnull(PP.QTY_PRO, 0) - ifnull(RE.QTY_RES, 0)
                   ) QTY_Available  ,
    QC.QCSOH QtyQC,
    GR.QtyOut Qty_GitRec,
    GR.QtyOut * I.UNIT_COST Cost_GitRec,
    COALESCE(PRICV.UnitPrice, pric.UnitPrice, I.UNIT_PRICE) * GR.QtyOut SellEx_GitRec,

    PP.QTY_PRO Qty_PRO,
    RE.QTY_RES Qty_RES,
    Onpo.OutstandingQuantity Qty_OnOrder,
    ONPO.OutstandingCost Cost_OnOrder,
    ONPO.OutstandingRspIncl SellEx_OnOrder,
    COALESCE(PRICV.UnitPrice, pric.UnitPrice, I.UNIT_PRICE) ActiveVariantUnitPrice,
     COALESCE(PRICV.OnMD, PRIC.OnMD)  OnMD,
    FRD.FirstRecDate,
	LRD.LastRecDate,
	LTROD.LastTRSDate,
	LSD.LastSoldDate, 
	     datediff(dd,try_to_date(  LSD.LastSoldDate ), getdate()) DaysSinceLastSold,
	   datediff(dd,try_to_date(  LTROD.LastTRSDate) , getdate()) DaysSinceLastTRS,
    datediff(week, try_to_date(  FRD.FirstRecDate ), getdate()) WksExposed,
    datediff(week, try_to_date(  LRD.LastRecDate ), getdate()) WksSinceLastRec,
  --  case
  --     when STA.IsWarehouse = 'True' then
  --         LS.QtyOnHand
  --     else
  --         null
  -- end QTY_DC,
    SOR.SOR QTY_SOR,
    COALESCE(PRICV.UnitPrice, pric.UnitPrice, I.UNIT_PRICE) * SOR.SOR SellEx_SOR,
    SOR.SOR * I.UNIT_COST Cost_SOR,
    case when (ifnull(QtyOnHand,0) + ifnull(GR.QtyOut,0)) <> 0 then 'YES' else 'NO' end HasStock,
    L8W.QtySoldL8Weeks,
    ILES.QtyReceived,
    ILES.QtySold,
    case 
						when DateDiff(DD,try_to_date(LRD.LastRecDate ), GETDATE()) <= 30 then 'L_Rec (0-30D)'
						 when DateDiff(DD,try_to_date(LRD.LastRecDate ), GETDATE()) > 30 and DateDiff(DD,LRD.LastRecDate, GETDATE()) <= 90  then 'L_Rec (1-3M)'
						 when DateDiff(DD,try_to_date(LRD.LastRecDate ), GETDATE()) > 90 and DateDiff(DD,LRD.LastRecDate, GETDATE()) <= 180  then 'L_Rec (4-6M)'
						 when DateDiff(DD,try_to_date(LRD.LastRecDate ), GETDATE()) > 180 and DateDiff(DD,LRD.LastRecDate, GETDATE()) <= 270  then 'L_Rec (7-9M)'
						 when DateDiff(DD,try_to_date(LRD.LastRecDate ), GETDATE()) > 270 and DateDiff(DD,LRD.LastRecDate, GETDATE()) <= 364  then 'L_Rec (10-12M)'
						 when DateDiff(DD,try_to_date(LRD.LastRecDate ), GETDATE()) > 364 and DateDiff(DD,LRD.LastRecDate, GETDATE()) <= 544  then 'L_Rec (13-18M)'
						 when DateDiff(DD,try_to_date(LRD.LastRecDate ), GETDATE()) > 544 and DateDiff(DD,LRD.LastRecDate, GETDATE()) <= 728  then 'L_Rec (18-24M)'
						 when DateDiff(DD,try_to_date(LRD.LastRecDate ), GETDATE()) > 728    then 'L_Rec (24M+)' else 'Not received yet'

	end L_RecAgeing , 
    
   case 
						when DateDiff(DD,try_to_date(LRD.LastRecDate), GETDATE()) <= 30 then 1
						 when DateDiff(DD,try_to_date(LRD.LastRecDate), GETDATE()) > 30 and DateDiff(DD,LRD.LastRecDate, GETDATE()) <= 90  then 2
						 when DateDiff(DD,try_to_date(LRD.LastRecDate), GETDATE()) > 90 and DateDiff(DD,LRD.LastRecDate, GETDATE()) <= 180  then 3
						 when DateDiff(DD,try_to_date(LRD.LastRecDate), GETDATE()) > 180 and DateDiff(DD,LRD.LastRecDate, GETDATE()) <= 270  then 4
						 when DateDiff(DD,try_to_date(LRD.LastRecDate), GETDATE()) > 270 and DateDiff(DD,LRD.LastRecDate, GETDATE()) <= 364  then 5
						 when DateDiff(DD,try_to_date(LRD.LastRecDate), GETDATE()) > 364 and DateDiff(DD,LRD.LastRecDate, GETDATE()) <= 544  then 6
						 when DateDiff(DD,try_to_date(LRD.LastRecDate), GETDATE()) > 544 and DateDiff(DD,LRD.LastRecDate, GETDATE()) <= 728  then 7
						 when DateDiff(DD,try_to_date(LRD.LastRecDate), GETDATE()) > 728    then 8 else 9

	end  L_RecAgeingSort   ,


	
   case 
						when DateDiff(DD,try_to_date(LSD.LastSoldDate), GETDATE()) <= 30 then 'L_Sold (0-30D)'
						 when DateDiff(DD,try_to_date(LSD.LastSoldDate), GETDATE()) > 30 and DateDiff(DD,LSD.LastSoldDate, GETDATE()) <= 90  then 'L_Sold (1-3M)'
						 when DateDiff(DD,try_to_date(LSD.LastSoldDate), GETDATE()) > 90 and DateDiff(DD,LSD.LastSoldDate, GETDATE()) <= 180  then 'L_Sold (4-6M)'
						 when DateDiff(DD,try_to_date(LSD.LastSoldDate), GETDATE()) > 180 and DateDiff(DD,LSD.LastSoldDate, GETDATE()) <= 270  then 'L_Sold (7-9M)'
						 when DateDiff(DD,try_to_date(LSD.LastSoldDate), GETDATE()) > 270 and DateDiff(DD,LSD.LastSoldDate, GETDATE()) <= 364  then 'L_Sold (10-12M)'
						 when DateDiff(DD,try_to_date(LSD.LastSoldDate), GETDATE()) > 364 and DateDiff(DD,LSD.LastSoldDate, GETDATE()) <= 544  then 'L_Sold (13-18M)'
						 when DateDiff(DD,try_to_date(LSD.LastSoldDate), GETDATE()) > 544 and DateDiff(DD,LSD.LastSoldDate, GETDATE()) <= 728  then 'L_Sold (18-24M)'
						 when DateDiff(DD,try_to_date(LSD.LastSoldDate), GETDATE()) > 728    then 'L_Sold (24M+)' else 'Not sold yet'

	end  L_SoldAgeing ,
    
     case 
						when DateDiff(DD,try_to_date(LSD.LastSoldDate), GETDATE()) <= 30 then 1
						 when DateDiff(DD,try_to_date(LSD.LastSoldDate), GETDATE()) > 30 and DateDiff(DD,LSD.LastSoldDate, GETDATE()) <= 90  then 2
						 when DateDiff(DD,try_to_date(LSD.LastSoldDate), GETDATE()) > 90 and DateDiff(DD,LSD.LastSoldDate, GETDATE()) <= 180  then 3
						 when DateDiff(DD,try_to_date(LSD.LastSoldDate), GETDATE()) > 180 and DateDiff(DD,LSD.LastSoldDate, GETDATE()) <= 270  then 4
						 when DateDiff(DD,try_to_date(LSD.LastSoldDate), GETDATE()) > 270 and DateDiff(DD,LSD.LastSoldDate, GETDATE()) <= 364  then 5
						 when DateDiff(DD,try_to_date(LSD.LastSoldDate), GETDATE()) > 364 and DateDiff(DD,LSD.LastSoldDate, GETDATE()) <= 544  then 6
						 when DateDiff(DD,try_to_date(LSD.LastSoldDate), GETDATE()) > 544 and DateDiff(DD,LSD.LastSoldDate, GETDATE()) <= 728  then 7
						 when DateDiff(DD,try_to_date(LSD.LastSoldDate), GETDATE()) > 728    then 8 else 9

	end L_SoldAgeingSort ,


	
  case 
						when DateDiff(DD,try_to_date(ltrod.LastTRSDate), GETDATE()) <= 30 then 'L_TROut (0-30D)'
						 when DateDiff(DD,try_to_date(ltrod.LastTRSDate), GETDATE()) > 30 and DateDiff(DD,ltrod.LastTRSDate, GETDATE()) <= 90  then 'L_TROut (1-3M)'
						 when DateDiff(DD,try_to_date(ltrod.LastTRSDate), GETDATE()) > 90 and DateDiff(DD,ltrod.LastTRSDate, GETDATE()) <= 180  then 'L_TROut (4-6M)'
						 when DateDiff(DD,try_to_date(ltrod.LastTRSDate), GETDATE()) > 180 and DateDiff(DD,ltrod.LastTRSDate, GETDATE()) <= 270  then 'L_TROut (7-9M)'
						 when DateDiff(DD,try_to_date(ltrod.LastTRSDate), GETDATE()) > 270 and DateDiff(DD,ltrod.LastTRSDate, GETDATE()) <= 364  then 'L_TROut (10-12M)'
						 when DateDiff(DD,try_to_date(ltrod.LastTRSDate), GETDATE()) > 364 and DateDiff(DD,ltrod.LastTRSDate, GETDATE()) <= 544  then 'L_TROut (13-18M)'
						 when DateDiff(DD,try_to_date(ltrod.LastTRSDate), GETDATE()) > 544 and DateDiff(DD,ltrod.LastTRSDate, GETDATE()) <= 728  then 'L_TROut (18-24M)'
						 when DateDiff(DD,try_to_date(ltrod.LastTRSDate), GETDATE()) > 728    then 'L_TROut (24M+)' else 'Not transferred yet'

	end   L_TROAgeing ,
    
     case 
						when DateDiff(DD,try_to_date(ltrod.LastTRSDate), GETDATE()) <= 30 then 1
						 when DateDiff(DD,try_to_date(ltrod.LastTRSDate), GETDATE()) > 30 and DateDiff(DD,ltrod.LastTRSDate, GETDATE()) <= 90  then 2
						 when DateDiff(DD,try_to_date(ltrod.LastTRSDate), GETDATE()) > 90 and DateDiff(DD,ltrod.LastTRSDate, GETDATE()) <= 180  then 3
						 when DateDiff(DD,try_to_date(ltrod.LastTRSDate), GETDATE()) > 180 and DateDiff(DD,ltrod.LastTRSDate, GETDATE()) <= 270  then 4
						 when DateDiff(DD,try_to_date(ltrod.LastTRSDate), GETDATE()) > 270 and DateDiff(DD,ltrod.LastTRSDate, GETDATE()) <= 364  then 5
						 when DateDiff(DD,try_to_date(ltrod.LastTRSDate), GETDATE()) > 364 and DateDiff(DD,ltrod.LastTRSDate, GETDATE()) <= 544  then 6
						 when DateDiff(DD,try_to_date(ltrod.LastTRSDate), GETDATE()) > 544 and DateDiff(DD,ltrod.LastTRSDate, GETDATE()) <= 728  then 7
						 when DateDiff(DD,try_to_date(ltrod.LastTRSDate), GETDATE()) > 728    then 8 else 9

	end L_TROAgeingSort ,
    WSS.Current_Week,
    WSS.Week_1_Ago,
    WSS.Week_2_Ago,
    WSS.Week_3_Ago,
    WSS.Week_4_Ago,
    WSS.Week_5_Ago,
    WSS.Week_6_Ago,
    WSS.Week_7_Ago,
    WSS.Total_8_Weeks,
    WSS.Avg_Weekly_Sales,
    WSS.Weeks_With_Sales  
    
FROM Loc
    left join LS
    on LS.CompanyName = Loc.CompanyName
        and LS.LocationCode = Loc.LocationCode
        and Loc.ItemNo = LS.ItemNo
        and Loc.Variant = LS.VariantCode
    left join TRO T
    on T.CompanyName = Loc.CompanyName
        and T.LocationFrom = Loc.LocationCode
        and T.ItemNo = Loc.ItemNo
        and T.Variant = Loc.Variant
    left join SO S
    on S.CompanyName = Loc.CompanyName
        and S.LocationCode = Loc.LocationCode
        and S.ItemNo = Loc.ItemNo
        and S.Variant = Loc.Variant
    LEFT JOIN fco.gold.vDimLocation DL
    ON DL.CompanyName = Loc.CompanyName
        AND DL.LocationCode = Loc.LocationCode
  --  left join vDimStoreAttributes STA
  --  on STA.StoreID = DL.StoreID
    LEFT JOIN fco.gold.vDimItem I
    ON I.CompanyName = Loc.CompanyName
        AND I.no_ = Loc.ItemNo
    LEFT JOIN fco.gold.vdimItemVariant IV
    ON IV.CompanyName = Loc.CompanyName
        AND IV.ItemNo = Loc.ItemNo
        AND IV.Variant = Loc.Variant
    Left join QC
    on QC.CompanyName = Loc.CompanyName
        and QC.ItemNo = Loc.ItemNo
        and QC.LocationCode = Loc.LocationCode
        and QC.VariantCode = Loc.Variant
    left join gitRec GR
    on GR.ToLocation = Loc.LocationCode
        and GR.ItemNo = Loc.ItemNo
        and GR.VariantCode = Loc.Variant
        and GR.CompanyName = Loc.CompanyName
    left join PRO PP
    on PP.CompanyName = Loc.CompanyName
        and PP.LocationCode = Loc.LocationCode
        and PP.ItemNo = Loc.ItemNo
        and PP.VariantCode = Loc.Variant
    left join RES RE
    on RE.CompanyName = Loc.CompanyName
        and RE.LocationCode = Loc.LocationCode
        and RE.ItemNo = Loc.ItemNo
        and RE.VariantCode = Loc.Variant
    left join UP
    on UP.CompanyName = Loc.CompanyName
        and UP.ItemNo = Loc.ItemNo
        and UP.LocationCode = Loc.LocationCode
        and UP.VariantCode = Loc.Variant
    left join PRICV
    on PRICV.CompanyName = Loc.CompanyName
        and PRICV.ItemNo = Loc.ItemNo
        and PRICV.VariantCode = Loc.Variant
        and PRICV.RN = 1
    left join PRIC
    on PRIC.CompanyName = Loc.CompanyName
        and PRIC.ItemNo = Loc.ItemNo
        and PRIC.RN = 1
    left join FRD
    on FRD.CompanyName = Loc.CompanyName
        and FRD.ItemNo = Loc.ItemNo
        and FRD.LocationCode = Loc.LocationCode
        and FRD.VariantCode = Loc.Variant
    left join LRD
    on LRD.CompanyName = Loc.CompanyName
        and LRD.ItemNo = Loc.ItemNo
        and LRD.LocationCode = Loc.LocationCode
        and LRD.VariantCode = Loc.Variant
     left join lsd
    on lsd.CompanyName = Loc.CompanyName
        and LSD.ItemNo = Loc.ItemNo
        and LSD.LocationCode = Loc.LocationCode
        and LSD.VariantCode = Loc.Variant
             left join LTROD
    on LTROD.CompanyName = Loc.CompanyName
        and LTROD.ItemNo = Loc.ItemNo
        and LTROD.LocationCode = Loc.LocationCode
        and LTROD.VariantCode = Loc.Variant

    left join ILES
    on ILES.CompanyName = Loc.CompanyName
        and ILES.ItemNo = Loc.ItemNo
        and ILES.LocationCode = Loc.LocationCode
        and ILES.VariantCode = Loc.Variant


    left join ONPO
    on ONPO.CompanyName = Loc.CompanyName
        and ONPO.LocationCode = Loc.LocationCode
        and Onpo.ItemNo = Loc.ItemNo
        and Onpo.VariantCode = Loc.Variant
    left join Sls8WkSum L8W on L8W.CompanyName = Loc.CompanyName 
    and L8W.ItemNo = Loc.ItemNo 
    and L8W.VariantCode = Loc.Variant 
    and L8W.LocationCode = Loc.LocationCode
    left join SOR on SOR.CompanyName = Loc.CompanyName
    and SOR.ItemNo = Loc.ItemNo 
    and SOR.VariantCode = Loc.Variant 
    and SOR.TransfertoCode = Loc.LocationCode
    left join WeeklySalesSum WSS on WSS.companyName = Loc.companyName
    and WSS.ItemNo = Loc.ItemNo 
    and WSS.VariantCode = Loc.Variant 
    and WSS.LocationCode = Loc.LocationCode
where (
      GR.ItemNo is not null
    or PP.ItemNo is not null
    or RE.ItemNo is not NULL
    or UP.ItemNo is not null
    or LS.CostAmountTotal <> 0
    or LS.QtyOnHand <> 0
    or LS.QtyRemaining <> 0
    or (ifnull(LS.QtyOnHand, 0) - ifnull(QC.QCSOH, 0) - ifnull(T.Qty_TO, 0) - ifnull(S.Qty_SO, 0)
          + ifnull(UP.Quantity, 0) + ifnull(PP.QTY_PRO, 0) - ifnull(RE.QTY_RES, 0)
         ) <> 0
  );

create or replace view FCO.GOLD.FACTCURRENTSTOCK_Z(
	COMPANYNAME,
	LOCATIONCODE,
	LOCATIONNAME,
	DIVISION,
	CATEGORY,
	SUBCATEGORY,
	ITEMNO,
	ITEMDESCRIPTION,
	ITEMVARIANT,
	COLOUR,
	SIZE,
	UNITPRICE,
	QTYONHAND,
	QTYREMAINING,
	COSTAMOUNTACTUAL,
	COSTAMOUNTEXPECTED,
	COSTAMOUNTTOTAL,
	QTYUNPOSTED,
	QTYONSO,
	QTYONTO,
	QTY_AVAILABLE,
	QTYQC,
	QTY_GITREC,
	COST_GITREC,
	SELLEX_GITREC,
	QTY_PRO,
	QTY_RES,
	QTY_ONORDER,
	COST_ONORDER,
	SELLEX_ONORDER,
	ACTIVEVARIANTUNITPRICE,
	ONMD,
	FIRSTRECDATE,
	LASTRECDATE,
	LASTTRSDATE,
	LASTSOLDDATE,
	DAYSSINCELASTSOLD,
	DAYSSINCELASTTRS,
	WKSEXPOSED,
	WKSSINCELASTREC,
	QTY_SOR,
	SELLEX_SOR,
	COST_SOR,
	HASSTOCK,
	QTYSOLDL8WEEKS,
	QTYRECEIVED,
	QTYSOLD,
	L_RECAGEING,
	L_RECAGEINGSORT,
	L_SOLDAGEING,
	L_SOLDAGEINGSORT,
	L_TROAGEING,
	L_TROAGEINGSORT,
	CSOH_CW,
	QSOLD_CW,
	CSOH_CW1,
	QSOLD_CW1,
	CSOH_CW2,
	QSOLD_CW2,
	CSOH_CW3,
	QSOLD_CW3,
	CSOH_CW4,
	QSOLD_CW4,
	CSOH_CW5,
	QSOLD_CW5,
	CSOH_CW6,
	QSOLD_CW6,
	CSOH_CW7,
	QSOLD_CW7,
	QSOLD_8WKS,
	AVG_QSOLD_8WKS,
	WEEKS_WITH_SOH,
	WEEKS_WITH_QSOLD
) as
            WITH
    UP --Done
    AS
    (

        SELECT TSE.CompanyName,
            DL.LocationCode LocationCode,
            TSE.itemNo  ,
            TSE.variantCode VariantCode,
            SUM(TSE.Quantity) Quantity
       FROM fco.silver.factlsctrans_salesentry TSE  
            LEFT JOIN  fco.silver.factlsctrans_salesentrystatus TSES  
            ON TSES.transactionNo = TSE.transactionNo
                AND TSES.storeNo  = TSE.storeNo
                AND TSES.itemNo   = TSE.itemNo
                AND TSES.lineNo = TSE.lineNo
                AND TSES.variantCode  = TSE.variantCode
                and TSES.CompanyName = TSE.CompanyName
            left join fco.silver.dimlocation SL  
            on SL.CompanyName = TSE.CompanyName
                and SL.code = TSE.storeNo
            left join fco.gold.vdimlocation DL  
            on DL.CompanyName = SL.CompanyName
                and DL.LocationCode = SL.code
        WHERE TSES.status != 'Posted'
            OR TSES.transactionNo IS NULL
        GROUP BY TSE.CompanyName,
        DL.LocationCode,
        TSE.itemNo,
        DL.LocationCode,
        TSE.variantCode
    ),


    TRO --done
as
(
    SELECT a.CompanyName,
        b.transferFromCode LocationFrom,
        b.itemNo AS ItemNo,
        b.variantCode AS Variant,
        SUM(b.outstandingQuantity) AS Qty_TO
FROM fco.silver.factTransferHeader a
        LEFT JOIN fco.silver.factTransferLine b 
        ON a.no = b.documentNo
        and a.CompanyName = b.CompanyName
 where a.Status = 'Released'
    GROUP BY a.CompanyName,
     b.transferFromCode,
     b.itemNo,
     b.variantCode
    HAVING SUM(b.outstandingQuantity) <> 0
),

QC --done
as
(
    SELECT CompanyName,
        itemNo  ,
        variantCode VariantCode,
        lastLocationCode LocationCode,
        SUM(quantity) QCSOH
    FROM fco.silver.factLotNo_Information  
    where qcRequired = 'True'
    GROUP BY CompanyName,
     itemNo,
     variantCode,
     lastLocationCode
),
        SO --Done
        as
        (
            SELECT a.CompanyName,
                a.locationCode LocationCode,
                b.no AS ItemNo,
                b.variantCode AS Variant,
                SUM(   CASE
                      WHEN a.documentType = 'Return Order' THEN
                          b.outstandingQuantity * -1
                      ELSE
                          b.outstandingQuantity
                  END
              ) AS Qty_SO
     FROM fco.silver.factSalesHeader a 
                LEFT JOIN fco.silver.factSalesLine b 
                ON a.no = b.documentNo
            WHERE b.Type = 'Item'
                and a.Status = 'Released'
                AND a.documentType IN ( 'Order', 'Return Order' )
            GROUP BY a.CompanyName,
             a.locationCode,
             b.no,
             b.variantCode
            HAVING SUM(b.outstandingQuantity) <> 0
        ),
        gitRec --done
        as
        (
            SELECT TH.CompanyName,
                COALESCE(TH.transferToCode, TL.transferToCode) ToLocation,
                TL.itemNo  ,
                TL.variantCode VariantCode,
                sum(TL.quantityShipped - TL.quantityReceived) as QtyOut
            FROM fco.silver.factTransferHeader TH  
 
                LEFT JOIN fco.silver.factTransferLine TL  
                ON TL.CompanyName = TH.CompanyName
                    AND TL.documentNo = TH.no
            group by TH.CompanyName,
            COALESCE(TH.transferToCode, TL.transferToCode),
            TL.itemNo, 
            TL.variantCode
        ),
        SOR --done
as
(

    SELECT
        h.CompanyName,
        l.transferToCode ,
        l.itemNo ,
        l.variantCode ,
        CAST(l.Quantity - l.quantityShipped AS INT) SOR

    FROM fco.silver.factTransferHeader h
        LEFT JOIN fco.silver.factTransferLine l
        ON h.CompanyName = l.CompanyName
            AND h.no = l.documentNo

    WHERE h.Status = 'Released' AND
        l.quantity - l.quantityShipped <> 0 AND
        l.derivedFromLineNo = 0

),




        PRO
        as
        (
            select PH.CompanyName,
                PL.locationCode LocationCode,
                PL.no ItemNo,
                Pl.variantCode VariantCode,
                sum(Quantity) QTY_PRO
            from fco.silver.factPurchaseHeader PH
                left join fco.silver.factPurchaseLine PL
                on PL.documentNo = PH.no
                and PL.CompanyName = PH.companyName
            where PH.documentType in  ('Return Order' )
                and PH.Status = 'Released'
            group by PH.CompanyName,
             PL.locationCode,
             PL.no,
             Pl.variantCode
            HAVING sum(quantity) <> 0
        ),

        
        RES --done
        as
        (
  SELECT CompanyName,
                locationCode ,
                itemNo ItemNo,
                variantCode ,
                CASE
               WHEN SUM(quantity) < 0 THEN
                   SUM(quantity) * -1
               ELSE
                   SUM(quantity)
           END QTY_RES
            FROM fco.silver.factReservationEntry RE
            WHERE Positive = 'False'
            and sourceID not in (select no from fco.silver.factTransferHeader where Status = 'Released')
            and sourceID not in (select no from fco.silver.factSalesHeader where Status = 'Released')
            GROUP BY CompanyName,
             locationCode,
             itemNo,
             variantCode
            HAVING sum(quantity) <> 0
        ),
        PRICV --done
        as
        (
            SELECT SPI.CompanyName,
                SPI.itemNo  ,
                SPI.variantCode  ,
                SPI.startingDate  ,
                SPI.endingDate ,
                SPI.unitPrice  ,
                SPI.onMarkdown OnMD,
                ROW_NUMBER() OVER (PARTITION BY SPI.CompanyName,
                                           SPI.itemNo,
                                           SPI.variantCode
                              ORDER BY SPI.CompanyName,
                                       SPI.itemNo,
                                       SPI.variantCode,
                                       SPI.startingDate DESC
                             ) AS RN
            from fco.silver.factSalesPrice SPI 
              
            where try_to_date( SPI.startingDate ) < getdate()
                AND SPI.salesCode = 'ALL'
                AND (
                  try_to_date( SPI.startingDate ) > GETDATE()
                OR try_to_date( SPI.startingDate ) = '0001-01-01'
              )
            --  and [Unit of Measure Code] = 'EACH'  
        ),
        PRIC --done
        as
        (
             SELECT SPI.CompanyName,
                SPI.itemNo  ,
                SPI.startingDate  ,
                SPI.endingDate  ,
                SPI.unitPrice  ,
                SPI.onMarkdown OnMD ,
                ROW_NUMBER() OVER (PARTITION BY SPI.CompanyName,
                                           SPI.itemNo
                              ORDER BY SPI.CompanyName,
                                       SPI.itemNo,
                                       try_to_date( SPI.startingDate ) DESC
                             ) AS RN
            from fco.silver.factSalesPrice SPI  
            where try_to_date( SPI.startingDate ) < getdate()
                AND SPI.salesCode = 'ALL'
                AND (
                  try_to_date(SPI.endingDate) > GETDATE()
                OR try_to_date( SPI.endingDate) = '0001-01-01'
              )
                -- and [Unit of Measure Code] = 'EACH'
                and SPI.variantCode is null
        ),
        ONPO --Done
        as
        (
            select PH.CompanyName,
                PL.locationCode  ,
                PL.no ItemNo,
                PL.variantCode  ,
                sum(PL.outstandingQuantity) outstandingQuantity ,
                sum(unitCostLCY * outstandingQuantity) OutstandingCost,
                sum(PL.unitPriceLCY * outstandingQuantity) OutstandingRspIncl
            FROM fco.silver.factPurchaseHeader PH  
              
                LEFT JOIN fco.silver.factPurchaseLine PL  
                ON PL.CompanyName = PH.CompanyName
                    AND PL.documentNo = PH.no
                    AND PL.documentType = PH.documentType
            where PH.documentType = 'Order'
                and outstandingQuantity > 0
                and status = 'Released'
         
                and lscRetailStatus in ('New',
'Part. receipt',
'Sent')
            group by PH.CompanyName,
             PL.no,
             PL.variantCode,
             PL.locationCode
        )

        ,
 
     frd 
     as
     (
         select CompanyName,
             locationCode  ,
             itemNo  ,
             variantCode  ,
             min(try_to_date(postingDate) ) as FirstRecDate
         from fco.silver.factItemLedgerEntry LE  
         where Quantity > 0
         group by CompanyName,
          CompanyName,
          locationCode,
          itemNo,
          variantCode
     )  ,
     lrd
        as
        (
            select CompanyName,
                locationCode  ,
                itemNo ,
                variantCode  ,
                 max(try_to_date(postingDate)) as LastRecDate
            from fco.silver.factItemLedgerEntry LE
            where  (LE.entryType = 'Purchase' and documentType = ' ') or (LE.entryType = 'Transfer' and documentType = 'Transfer_x0020_Receipt')
            or (LE.entryType = 'Positive_x0020_Adjmt_x002E_' )
            group by CompanyName,
             CompanyName,
             locationCode,
             itemNo,
             variantCode
        ),
        lsd
        as
        (
            select CompanyName,
                locationCode  ,
                itemNo  ,
                variantCode  ,
                 max(try_to_date(postingDate)) as LastSoldDate
            from fco.silver.factItemLedgerEntry LE
            where  (LE.entryType = 'Sale' and documentType in ('_x0020_','Sales_x0020_Shipment')  ) 
            group by CompanyName,
             CompanyName,
             locationCode,
             itemNo,
             variantCode
        ),
        ltrod
        as
        (
            select CompanyName,
                locationCode  ,
                itemNo  ,
                variantCode  ,
                 max(try_to_date(postingDate)) as LastTRSDate
            from fco.silver.factItemLedgerEntry LE
            where  (LE.entryType = 'Transfer' and documentType = 'Transfer_x0020_Shipment')  
            group by CompanyName,
             CompanyName,
             locationCode,
             itemNo,
             variantCode
        ),
--done
ILES as (

    select CompanyName,
                locationCode   ,
                itemNo  ,
                variantCode   ,
                sum(case when entryType = 'Purchase' or (entryType = 'Transfer' and documentType = 'Transfer_x0020_Receipt') 
                or (entryType = 'Positive_x0020_Adjmt_x002E_' and documentType = ' ') then  Quantity  else null end ) as QtyReceived,
                sum(case when entryType = 'Sale' then  Quantity * -1  else null end ) as QtySold
           from fco.silver.factItemLedgerEntry LE
        group by  
        CompanyName,
                locationCode,
                itemNo,
                variantCode 

),
        SL8Wks
        as
        (

        --done
                                        SELECT H.CompanyName,
                    H.ItemNo,
                    H.VariantCode,
                    SUM(H.Quantity) * - 1 Quantity,
                    H.LocationCode
           FROM fco.gold.vFactSalesCreditMemo H  
                  --  LEFT JOIN [saas].[vDimSalesPersonPurchaser] SP
                  --  ON H.[Salesperson Code] = SP.[Salesperson Code] AND H.CompanyName = SP.CompanyName
                 --   left join vDimSalesStaff SS on SS.CompanyName = H.CompanyName
                  --      and SS.[Sales Staff ID] = H.[Salesperson Code]

                WHERE H.LocationCode <> ''
                    AND H.Type = 'Item'
                    AND H.Quantity != 0
                    and PostingDate > DATEADD(DD,-56,GETDATE())
                GROUP BY 
              H.CompanyName,H.VariantCode, 
             H.ItemNo, 
             H.LocationCode
            union all
            --done
         --       SELECT H.CompanyName,
         --           H.[Item No_] [Item No_],
         --           H.[Variant Code],
        --            SUM(H.Quantity) Quantity,
        --            H.[Location Code]
        --        FROM saas.vFactSalesInvoice H WITH (NOLOCK)
        --            LEFT JOIN [saas].[vDimSalesPersonPurchaser] SP
        --            ON H.[Salesperson Code] = SP.[Salesperson Code] AND H.CompanyName = SP.CompanyName
        --            left join vDimSalesStaff SS on SS.CompanyName = H.CompanyName
        --                and SS.[Sales Staff ID] = H.[Salesperson Code]

        --        WHERE H.[Location Code] <> ''
        --            AND H.Type = 'Item'
        --            AND H.Quantity != 0
        --            and H.[Posting Date] >= convert(Date,DATEADD(DD,-56,getdate()))

        --        GROUP BY
        --    H.CompanyName, H.[Variant Code],
        --     H.[Item No_],
        --     H.[Location Code]

        --    union all
            --done
                SELECT LSE.CompanyName, itemNo , variantCode , sum(Quantity * -1), DL.LocationCode
                from fco.silver.factLSCTrans_SalesEntry LSE  
                    left join fco.gold.vdimstore DS on DS.StoreNo = LSE.storeNo  
                    and DS.CompanyName = LSE.CompanyName  
                    left join fco.gold.vdimlocation DL on DL.StoreID = DS.StoreSKey 
                where try_to_date(LSE.Date) >= DATEADD(DD,-56,getdate())
                group by LSE.CompanyName,itemNo, LSE.variantCode,  DL.LocationCode

  

        ),
        --done
        Sls8WkSum
        as
        (
            select CompanyName, ItemNo, VariantCode, LocationCode, sum(Quantity) QtySoldL8Weeks
            from SL8Wks
            group by  CompanyName, ItemNo, VariantCode, LocationCode

        )    ,
    --done


 WeeklySales AS (
    SELECT 
        LSE.CompanyName,
        LSE.itemNo,
        LSE.variantCode,
        DL.LocationCode,
        -- Calculate week starting date (Sunday)
        DATE_TRUNC('WEEK', try_to_date(LSE.Date)) AS WeekStartDate,
        -- Week number for reference
        WEEK(try_to_date(LSE.Date)) AS WeekNumber,
        YEAR(try_to_date(LSE.Date)) AS Year,
        -- Friendly week label for column naming
        CONCAT('Week_', TO_CHAR(DATE_TRUNC('WEEK', try_to_date(LSE.Date)), 'MM_DD_YYYY')) AS WeekLabel,
        -- Sales quantity (assuming negative values need to be made positive)
        SUM(LSE.Quantity * -1) AS WeeklySalesQuantity
    FROM fco.silver.factLSCTrans_SalesEntry LSE  
        LEFT JOIN fco.gold.vdimstore DS 
            ON DS.StoreNo = LSE.storeNo  
            AND DS.CompanyName = LSE.CompanyName  
        LEFT JOIN fco.gold.vdimlocation DL 
            ON DL.StoreID = DS.StoreSKey 
    WHERE 
        -- Filter for last 8 weeks (Sunday to Saturday)
        LSE.Date >= DATE_TRUNC('WEEK', DATEADD('WEEK', -8, CURRENT_DATE()))
        AND LSE.Date < DATE_TRUNC('WEEK', DATEADD('WEEK', 1, CURRENT_DATE()))
        -- Add any additional filters you need
        -- AND LSE.CompanyName = 'YourCompanyName'
        -- AND DL.LocationCode IN ('LOC1', 'LOC2')
    GROUP BY 
        LSE.CompanyName,
        LSE.itemNo, 
        LSE.variantCode,  
        DL.LocationCode,
        DATE_TRUNC('WEEK', try_to_date(LSE.Date)),
        WEEK(try_to_date(LSE.Date)),
        YEAR(try_to_date(LSE.Date))
),

WeeklySalesSum
as

(SELECT 
    CompanyName,
    itemNo,
    variantCode,
    LocationCode,
    -- Pivot columns for each of the last 8 weeks (most recent first)
    SUM(CASE WHEN WeekStartDate = DATE_TRUNC('WEEK', CURRENT_DATE()) 
             THEN WeeklySalesQuantity ELSE 0 END) AS QSOLD_CW,
    
    SUM(CASE WHEN WeekStartDate = DATE_TRUNC('WEEK', DATEADD('WEEK', -1, CURRENT_DATE())) 
             THEN WeeklySalesQuantity ELSE 0 END) AS QSOLD_CW1,
    
    SUM(CASE WHEN WeekStartDate = DATE_TRUNC('WEEK', DATEADD('WEEK', -2, CURRENT_DATE())) 
             THEN WeeklySalesQuantity ELSE 0 END) AS QSOLD_CW2,
    
    SUM(CASE WHEN WeekStartDate = DATE_TRUNC('WEEK', DATEADD('WEEK', -3, CURRENT_DATE())) 
             THEN WeeklySalesQuantity ELSE 0 END) AS QSOLD_CW3,
    
    SUM(CASE WHEN WeekStartDate = DATE_TRUNC('WEEK', DATEADD('WEEK', -4, CURRENT_DATE())) 
             THEN WeeklySalesQuantity ELSE 0 END) AS QSOLD_CW4,
    
    SUM(CASE WHEN WeekStartDate = DATE_TRUNC('WEEK', DATEADD('WEEK', -5, CURRENT_DATE())) 
             THEN WeeklySalesQuantity ELSE 0 END) AS QSOLD_CW5,
    
    SUM(CASE WHEN WeekStartDate = DATE_TRUNC('WEEK', DATEADD('WEEK', -6, CURRENT_DATE())) 
             THEN WeeklySalesQuantity ELSE 0 END) AS QSOLD_CW6,
    
    SUM(CASE WHEN WeekStartDate = DATE_TRUNC('WEEK', DATEADD('WEEK', -7, CURRENT_DATE())) 
             THEN WeeklySalesQuantity ELSE 0 END) AS QSOLD_CW7,
    
    -- Additional useful columns
    SUM(WeeklySalesQuantity) AS QSOLD_8Wks,
    ROUND(AVG(WeeklySalesQuantity), 2) AS Avg_QSOLD_8Wks,
    COUNT(CASE WHEN WeeklySalesQuantity > 0 THEN 1 END) AS Weeks_With_QSOLD

FROM WeeklySales
GROUP BY 
    CompanyName,
    itemNo, 
    variantCode,  
    LocationCode
ORDER BY 
    CompanyName,
    LocationCode,
    itemNo,
    variantCode
),

weeklyClosingStock

as
(
SELECT 
    WS.CompanyName,
    DI.NO_ ItemNo,
    IV.variant variantCode,
    DL.LOCATIONCODE,
    -- Pivot columns for each of the last 8 weeks (most recent first)
    SUM(CASE WHEN try_to_date(to_char(dimdateid),'YYYYMMDD') = DATE_TRUNC('WEEK', CURRENT_DATE()) 
             THEN closingsoh ELSE 0 END) AS CSOH_CW,
             SUM(CASE WHEN try_to_date(to_char(dimdateid),'YYYYMMDD') = DATE_TRUNC('WEEK' , DATEADD('WEEK', -1, CURRENT_DATE())) 
             THEN closingsoh ELSE 0 END) AS CSOH_CW1,
             SUM(CASE WHEN try_to_date(to_char(dimdateid),'YYYYMMDD') = DATE_TRUNC('WEEK', DATEADD('WEEK', -2, CURRENT_DATE())) 
             THEN closingsoh ELSE 0 END) AS CSOH_CW2,
             SUM(CASE WHEN try_to_date(to_char(dimdateid),'YYYYMMDD') = DATE_TRUNC('WEEK', DATEADD('WEEK', -3, CURRENT_DATE())) 
             THEN closingsoh ELSE 0 END) AS CSOH_CW3,
             SUM(CASE WHEN try_to_date(to_char(dimdateid),'YYYYMMDD') = DATE_TRUNC('WEEK', DATEADD('WEEK', -4, CURRENT_DATE())) 
             THEN closingsoh ELSE 0 END) AS CSOH_CW4,
             SUM(CASE WHEN try_to_date(to_char(dimdateid),'YYYYMMDD') = DATE_TRUNC('WEEK', DATEADD('WEEK', -5, CURRENT_DATE())) 
             THEN closingsoh ELSE 0 END) AS CSOH_CW5,
             SUM(CASE WHEN try_to_date(to_char(dimdateid),'YYYYMMDD') = DATE_TRUNC('WEEK', DATEADD('WEEK', -6, CURRENT_DATE())) 
             THEN closingsoh ELSE 0 END) AS CSOH_CW6,
             SUM(CASE WHEN try_to_date(to_char(dimdateid),'YYYYMMDD') = DATE_TRUNC('WEEK', DATEADD('WEEK', -7, CURRENT_DATE())) 
             THEN closingsoh ELSE 0 END) AS CSOH_CW7,
    
    -- Additional useful columns

    ROUND(AVG(WS.CLOSINGSOH), 2) AS Avg_Weekly_CSOH,
    COUNT(CASE WHEN CLOSINGSOH > 0 THEN 1 END) AS Weeks_With_SOH

FROM fco.gold.tFactWeeklySkuStoreCLoseStock WS left join fco.gold.vdimlocation DL on DL.locationskey = WS.LOCATIONSKEY
left join fco.gold.vdimitem DI on DI.itemskey = WS.ITEMSKEY
left join fco.gold.vdimitemvariant IV on IV.ITEMVARIANTSKEY = WS.ITEMVARIANTSKEY
GROUP BY 
    WS.CompanyName,
    DI.NO_  ,
    IV.variant  ,
    DL.LOCATIONCODE
ORDER BY 
    WS.CompanyName,
    DI.NO_  ,
    IV.variant  ,
    DL.LOCATIONCODE
 

)



    ,
    LS
    as
    (
        select LE.CompanyName,
            LE.locationCode ,
            LE.itemNo  ,
            LE.variantCode  ,
            SUM(LE.Quantity) QtyOnHand,
            SUM(LE.remainingQuantity) QtyRemaining,
            SUM(VE.CostAmountActual) CostAmountActual,
            SUM(VE.CostAmountExpected) CostAmountExpected,
            SUM(VE.CostAmountExpected + VE.CostAmountActual) CostAmountTotal,
            0 UnitPrice
        from fco.silver.factItemLedgerEntry LE  
            LEFT JOIN fco.gold.vFactValueEntry_ILE VE 
            ON VE.CompanyName = LE.CompanyName
                AND VE.ItemNo = LE.itemNo
                AND VE.LocationCode = LE.locationCode
                AND VE.VariantCode = LE.variantCode
                AND VE.ITEMLEDGERENTRYNO = LE.entryNo
        group by LE.CompanyName,
         LE.locationCode,
         LE.itemNo,
         LE.variantCode
    ) ,
    --new werner all locations x join
    LocSum
    as
    (
                select CompanyName,
                LocationCode,
                ItemNo,
                VariantCode
            from LS
        union ALL
            select CompanyName,
                LocationCode,
                ItemNo,
                VariantCode
            from ONPO
        union all
            select CompanyName,
                LocationFrom,
                ItemNo,
                Variant
            from TRO
        union ALL
            select CompanyName,
                LocationCode,
                ItemNo,
                Variant
            from SO
        union ALL
            select CompanyName,
                LocationCode,
                ItemNo,
                VariantCode
            from QC
        union ALL
            select CompanyName,
                ToLocation,
                ItemNo,
                VariantCode
            from gitRec
        union ALL
            select CompanyName,
                LocationCode,
                ItemNo,
                VariantCode
            from PRO
        union ALL
            select CompanyName,
                LocationCode,
                ItemNo,
                VariantCode
            from RES
        union all
            select CompanyName,
                TransfertoCode,
                ItemNo,
                VariantCode
            from SOR
            union all
            select CompanyName,
                LocationCode,
                ItemNo,
                VariantCode from 
            WeeklySalesSum
            union all
            select CompanyName,
                LocationCode,
                ItemNo,
                VariantCode from 
            weeklyClosingStock


    ),
    loc
    as
    (
        select CompanyName,
            LocationCode,
            ItemNo,
            VariantCode Variant
        from LocSum
        group by CompanyName,
         LocationCode,
         ItemNo,
         VariantCode
    )
 
SELECT Loc.CompanyName, DL.LocationCode,
DL.LocationName,
DIVISION_CODE_DESCRIPTION Division,  
ITEM_CATEGORY_DESCRIPTION Category,
RETAIL_PRODUCT_DESCRIPTION SubCat,
    I.No_ ItemNo,
    I.DESCRIPTION ItemDescription,
    IV.VARIANT ItemVariant,
    IV.VARIANT_DIMENSION_1 Colour,
    IV.variant_dimension_2 Size,
    COALESCE(IV.ItemVariantUnitPrice, I.CURRENT_ACTIVE_PRICE_BTR) UnitPrice,
    LS.QtyOnHand,
    LS.QtyRemaining,
    LS.CostAmountActual,
    LS.CostAmountExpected,
    LS.CostAmountTotal,
    --isnull(UP.Quantity,0) QtyUnposted, isnull(S.Qty_SO,0) QtyOnSO, isnull(T.Qty_TO,0) QtyOnTO,
    UP.Quantity QtyUnposted,
    S.Qty_SO QtyOnSO,
    T.Qty_TO QtyOnTO,
     (ifnull(LS.QtyOnHand, 0) - ifnull(QC.QCSOH, 0) - ifnull(T.Qty_TO, 0) - ifnull(S.Qty_SO, 0)
                    + ifnull(UP.Quantity, 0) + ifnull(PP.QTY_PRO, 0) - ifnull(RE.QTY_RES, 0)
                   ) QTY_Available  ,
    QC.QCSOH QtyQC,
    GR.QtyOut Qty_GitRec,
    GR.QtyOut * I.UNIT_COST Cost_GitRec,
    COALESCE(PRICV.UnitPrice, pric.UnitPrice, I.UNIT_PRICE) * GR.QtyOut SellEx_GitRec,

    PP.QTY_PRO Qty_PRO,
    RE.QTY_RES Qty_RES,
    Onpo.OutstandingQuantity Qty_OnOrder,
    ONPO.OutstandingCost Cost_OnOrder,
    ONPO.OutstandingRspIncl SellEx_OnOrder,
    COALESCE(PRICV.UnitPrice, pric.UnitPrice, I.UNIT_PRICE) ActiveVariantUnitPrice,
     COALESCE(PRICV.OnMD, PRIC.OnMD)  OnMD,
    FRD.FirstRecDate,
	LRD.LastRecDate,
	LTROD.LastTRSDate,
	LSD.LastSoldDate, 
	     datediff(dd,try_to_date(  LSD.LastSoldDate ), getdate()) DaysSinceLastSold,
	   datediff(dd,try_to_date(  LTROD.LastTRSDate) , getdate()) DaysSinceLastTRS,
    datediff(week, try_to_date(  FRD.FirstRecDate ), getdate()) WksExposed,
    datediff(week, try_to_date(  LRD.LastRecDate ), getdate()) WksSinceLastRec,
  --  case
  --     when STA.IsWarehouse = 'True' then
  --         LS.QtyOnHand
  --     else
  --         null
  -- end QTY_DC,
    SOR.SOR QTY_SOR,
    COALESCE(PRICV.UnitPrice, pric.UnitPrice, I.UNIT_PRICE) * SOR.SOR SellEx_SOR,
    SOR.SOR * I.UNIT_COST Cost_SOR,
    case when (ifnull(QtyOnHand,0) + ifnull(GR.QtyOut,0)) <> 0 then 'YES' else 'NO' end HasStock,
    L8W.QtySoldL8Weeks,
    ILES.QtyReceived,
    ILES.QtySold,

 
    
    case 
						when DateDiff(DD,try_to_date(LRD.LastRecDate ), GETDATE()) <= 30 then 'L_Rec (0-30D)'
						 when DateDiff(DD,try_to_date(LRD.LastRecDate ), GETDATE()) > 30 and DateDiff(DD,LRD.LastRecDate, GETDATE()) <= 90  then 'L_Rec (1-3M)'
						 when DateDiff(DD,try_to_date(LRD.LastRecDate ), GETDATE()) > 90 and DateDiff(DD,LRD.LastRecDate, GETDATE()) <= 180  then 'L_Rec (4-6M)'
						 when DateDiff(DD,try_to_date(LRD.LastRecDate ), GETDATE()) > 180 and DateDiff(DD,LRD.LastRecDate, GETDATE()) <= 270  then 'L_Rec (7-9M)'
						 when DateDiff(DD,try_to_date(LRD.LastRecDate ), GETDATE()) > 270 and DateDiff(DD,LRD.LastRecDate, GETDATE()) <= 364  then 'L_Rec (10-12M)'
						 when DateDiff(DD,try_to_date(LRD.LastRecDate ), GETDATE()) > 364 and DateDiff(DD,LRD.LastRecDate, GETDATE()) <= 544  then 'L_Rec (13-18M)'
						 when DateDiff(DD,try_to_date(LRD.LastRecDate ), GETDATE()) > 544 and DateDiff(DD,LRD.LastRecDate, GETDATE()) <= 728  then 'L_Rec (18-24M)'
						 when DateDiff(DD,try_to_date(LRD.LastRecDate ), GETDATE()) > 728    then 'L_Rec (24M+)' else 'Not received yet'

	end L_RecAgeing , 
    
   case 
						when DateDiff(DD,try_to_date(LRD.LastRecDate), GETDATE()) <= 30 then 1
						 when DateDiff(DD,try_to_date(LRD.LastRecDate), GETDATE()) > 30 and DateDiff(DD,LRD.LastRecDate, GETDATE()) <= 90  then 2
						 when DateDiff(DD,try_to_date(LRD.LastRecDate), GETDATE()) > 90 and DateDiff(DD,LRD.LastRecDate, GETDATE()) <= 180  then 3
						 when DateDiff(DD,try_to_date(LRD.LastRecDate), GETDATE()) > 180 and DateDiff(DD,LRD.LastRecDate, GETDATE()) <= 270  then 4
						 when DateDiff(DD,try_to_date(LRD.LastRecDate), GETDATE()) > 270 and DateDiff(DD,LRD.LastRecDate, GETDATE()) <= 364  then 5
						 when DateDiff(DD,try_to_date(LRD.LastRecDate), GETDATE()) > 364 and DateDiff(DD,LRD.LastRecDate, GETDATE()) <= 544  then 6
						 when DateDiff(DD,try_to_date(LRD.LastRecDate), GETDATE()) > 544 and DateDiff(DD,LRD.LastRecDate, GETDATE()) <= 728  then 7
						 when DateDiff(DD,try_to_date(LRD.LastRecDate), GETDATE()) > 728    then 8 else 9

	end  L_RecAgeingSort   ,


	
   case 
						when DateDiff(DD,try_to_date(LSD.LastSoldDate), GETDATE()) <= 30 then 'L_Sold (0-30D)'
						 when DateDiff(DD,try_to_date(LSD.LastSoldDate), GETDATE()) > 30 and DateDiff(DD,LSD.LastSoldDate, GETDATE()) <= 90  then 'L_Sold (1-3M)'
						 when DateDiff(DD,try_to_date(LSD.LastSoldDate), GETDATE()) > 90 and DateDiff(DD,LSD.LastSoldDate, GETDATE()) <= 180  then 'L_Sold (4-6M)'
						 when DateDiff(DD,try_to_date(LSD.LastSoldDate), GETDATE()) > 180 and DateDiff(DD,LSD.LastSoldDate, GETDATE()) <= 270  then 'L_Sold (7-9M)'
						 when DateDiff(DD,try_to_date(LSD.LastSoldDate), GETDATE()) > 270 and DateDiff(DD,LSD.LastSoldDate, GETDATE()) <= 364  then 'L_Sold (10-12M)'
						 when DateDiff(DD,try_to_date(LSD.LastSoldDate), GETDATE()) > 364 and DateDiff(DD,LSD.LastSoldDate, GETDATE()) <= 544  then 'L_Sold (13-18M)'
						 when DateDiff(DD,try_to_date(LSD.LastSoldDate), GETDATE()) > 544 and DateDiff(DD,LSD.LastSoldDate, GETDATE()) <= 728  then 'L_Sold (18-24M)'
						 when DateDiff(DD,try_to_date(LSD.LastSoldDate), GETDATE()) > 728    then 'L_Sold (24M+)' else 'Not sold yet'

	end  L_SoldAgeing ,
    
     case 
						when DateDiff(DD,try_to_date(LSD.LastSoldDate), GETDATE()) <= 30 then 1
						 when DateDiff(DD,try_to_date(LSD.LastSoldDate), GETDATE()) > 30 and DateDiff(DD,LSD.LastSoldDate, GETDATE()) <= 90  then 2
						 when DateDiff(DD,try_to_date(LSD.LastSoldDate), GETDATE()) > 90 and DateDiff(DD,LSD.LastSoldDate, GETDATE()) <= 180  then 3
						 when DateDiff(DD,try_to_date(LSD.LastSoldDate), GETDATE()) > 180 and DateDiff(DD,LSD.LastSoldDate, GETDATE()) <= 270  then 4
						 when DateDiff(DD,try_to_date(LSD.LastSoldDate), GETDATE()) > 270 and DateDiff(DD,LSD.LastSoldDate, GETDATE()) <= 364  then 5
						 when DateDiff(DD,try_to_date(LSD.LastSoldDate), GETDATE()) > 364 and DateDiff(DD,LSD.LastSoldDate, GETDATE()) <= 544  then 6
						 when DateDiff(DD,try_to_date(LSD.LastSoldDate), GETDATE()) > 544 and DateDiff(DD,LSD.LastSoldDate, GETDATE()) <= 728  then 7
						 when DateDiff(DD,try_to_date(LSD.LastSoldDate), GETDATE()) > 728    then 8 else 9

	end L_SoldAgeingSort ,


	
  case 
						when DateDiff(DD,try_to_date(ltrod.LastTRSDate), GETDATE()) <= 30 then 'L_TROut (0-30D)'
						 when DateDiff(DD,try_to_date(ltrod.LastTRSDate), GETDATE()) > 30 and DateDiff(DD,ltrod.LastTRSDate, GETDATE()) <= 90  then 'L_TROut (1-3M)'
						 when DateDiff(DD,try_to_date(ltrod.LastTRSDate), GETDATE()) > 90 and DateDiff(DD,ltrod.LastTRSDate, GETDATE()) <= 180  then 'L_TROut (4-6M)'
						 when DateDiff(DD,try_to_date(ltrod.LastTRSDate), GETDATE()) > 180 and DateDiff(DD,ltrod.LastTRSDate, GETDATE()) <= 270  then 'L_TROut (7-9M)'
						 when DateDiff(DD,try_to_date(ltrod.LastTRSDate), GETDATE()) > 270 and DateDiff(DD,ltrod.LastTRSDate, GETDATE()) <= 364  then 'L_TROut (10-12M)'
						 when DateDiff(DD,try_to_date(ltrod.LastTRSDate), GETDATE()) > 364 and DateDiff(DD,ltrod.LastTRSDate, GETDATE()) <= 544  then 'L_TROut (13-18M)'
						 when DateDiff(DD,try_to_date(ltrod.LastTRSDate), GETDATE()) > 544 and DateDiff(DD,ltrod.LastTRSDate, GETDATE()) <= 728  then 'L_TROut (18-24M)'
						 when DateDiff(DD,try_to_date(ltrod.LastTRSDate), GETDATE()) > 728    then 'L_TROut (24M+)' else 'Not transferred yet'

	end   L_TROAgeing ,
    
     case 
						when DateDiff(DD,try_to_date(ltrod.LastTRSDate), GETDATE()) <= 30 then 1
						 when DateDiff(DD,try_to_date(ltrod.LastTRSDate), GETDATE()) > 30 and DateDiff(DD,ltrod.LastTRSDate, GETDATE()) <= 90  then 2
						 when DateDiff(DD,try_to_date(ltrod.LastTRSDate), GETDATE()) > 90 and DateDiff(DD,ltrod.LastTRSDate, GETDATE()) <= 180  then 3
						 when DateDiff(DD,try_to_date(ltrod.LastTRSDate), GETDATE()) > 180 and DateDiff(DD,ltrod.LastTRSDate, GETDATE()) <= 270  then 4
						 when DateDiff(DD,try_to_date(ltrod.LastTRSDate), GETDATE()) > 270 and DateDiff(DD,ltrod.LastTRSDate, GETDATE()) <= 364  then 5
						 when DateDiff(DD,try_to_date(ltrod.LastTRSDate), GETDATE()) > 364 and DateDiff(DD,ltrod.LastTRSDate, GETDATE()) <= 544  then 6
						 when DateDiff(DD,try_to_date(ltrod.LastTRSDate), GETDATE()) > 544 and DateDiff(DD,ltrod.LastTRSDate, GETDATE()) <= 728  then 7
						 when DateDiff(DD,try_to_date(ltrod.LastTRSDate), GETDATE()) > 728    then 8 else 9

	end L_TROAgeingSort ,
    WCS.CSOH_CW ,
    WSS.QSOLD_CW,
    WCS.CSOH_CW1 ,
    WSS.QSOLD_CW1,
    WCS.CSOH_CW2 ,
    WSS.QSOLD_CW2,
    WCS.CSOH_CW3 ,
    WSS.QSOLD_CW3,
    WCS.CSOH_CW4 ,
    WSS.QSOLD_CW4,
    WCS.CSOH_CW5 ,
    WSS.QSOLD_CW5,
    WCS.CSOH_CW6 ,
    WSS.QSOLD_CW6,
    WCS.CSOH_CW7 ,
    WSS.QSOLD_CW7,
    WSS.QSOLD_8Wks,
    WSS.Avg_QSOLD_8Wks,
    WCS.Weeks_With_SOH ,
    WSS.Weeks_With_QSOLD  
    
FROM Loc
    left join LS
    on LS.CompanyName = Loc.CompanyName
        and LS.LocationCode = Loc.LocationCode
        and Loc.ItemNo = LS.ItemNo
        and Loc.Variant = LS.VariantCode
    left join TRO T
    on T.CompanyName = Loc.CompanyName
        and T.LocationFrom = Loc.LocationCode
        and T.ItemNo = Loc.ItemNo
        and T.Variant = Loc.Variant
    left join SO S
    on S.CompanyName = Loc.CompanyName
        and S.LocationCode = Loc.LocationCode
        and S.ItemNo = Loc.ItemNo
        and S.Variant = Loc.Variant
    LEFT JOIN fco.gold.vDimLocation DL
    ON DL.CompanyName = Loc.CompanyName
        AND DL.LocationCode = Loc.LocationCode
  --  left join vDimStoreAttributes STA
  --  on STA.StoreID = DL.StoreID
    LEFT JOIN fco.gold.vDimItem I
    ON I.CompanyName = Loc.CompanyName
        AND I.no_ = Loc.ItemNo
    LEFT JOIN fco.gold.vdimItemVariant IV
    ON IV.CompanyName = Loc.CompanyName
        AND IV.ItemNo = Loc.ItemNo
        AND IV.Variant = Loc.Variant
    Left join QC
    on QC.CompanyName = Loc.CompanyName
        and QC.ItemNo = Loc.ItemNo
        and QC.LocationCode = Loc.LocationCode
        and QC.VariantCode = Loc.Variant
    left join gitRec GR
    on GR.ToLocation = Loc.LocationCode
        and GR.ItemNo = Loc.ItemNo
        and GR.VariantCode = Loc.Variant
        and GR.CompanyName = Loc.CompanyName
    left join PRO PP
    on PP.CompanyName = Loc.CompanyName
        and PP.LocationCode = Loc.LocationCode
        and PP.ItemNo = Loc.ItemNo
        and PP.VariantCode = Loc.Variant
    left join RES RE
    on RE.CompanyName = Loc.CompanyName
        and RE.LocationCode = Loc.LocationCode
        and RE.ItemNo = Loc.ItemNo
        and RE.VariantCode = Loc.Variant
    left join UP
    on UP.CompanyName = Loc.CompanyName
        and UP.ItemNo = Loc.ItemNo
        and UP.LocationCode = Loc.LocationCode
        and UP.VariantCode = Loc.Variant
    left join PRICV
    on PRICV.CompanyName = Loc.CompanyName
        and PRICV.ItemNo = Loc.ItemNo
        and PRICV.VariantCode = Loc.Variant
        and PRICV.RN = 1
    left join PRIC
    on PRIC.CompanyName = Loc.CompanyName
        and PRIC.ItemNo = Loc.ItemNo
        and PRIC.RN = 1
    left join FRD
    on FRD.CompanyName = Loc.CompanyName
        and FRD.ItemNo = Loc.ItemNo
        and FRD.LocationCode = Loc.LocationCode
        and FRD.VariantCode = Loc.Variant
    left join LRD
    on LRD.CompanyName = Loc.CompanyName
        and LRD.ItemNo = Loc.ItemNo
        and LRD.LocationCode = Loc.LocationCode
        and LRD.VariantCode = Loc.Variant
     left join lsd
    on lsd.CompanyName = Loc.CompanyName
        and LSD.ItemNo = Loc.ItemNo
        and LSD.LocationCode = Loc.LocationCode
        and LSD.VariantCode = Loc.Variant
             left join LTROD
    on LTROD.CompanyName = Loc.CompanyName
        and LTROD.ItemNo = Loc.ItemNo
        and LTROD.LocationCode = Loc.LocationCode
        and LTROD.VariantCode = Loc.Variant

    left join ILES
    on ILES.CompanyName = Loc.CompanyName
        and ILES.ItemNo = Loc.ItemNo
        and ILES.LocationCode = Loc.LocationCode
        and ILES.VariantCode = Loc.Variant


    left join ONPO
    on ONPO.CompanyName = Loc.CompanyName
        and ONPO.LocationCode = Loc.LocationCode
        and Onpo.ItemNo = Loc.ItemNo
        and Onpo.VariantCode = Loc.Variant
    left join Sls8WkSum L8W on L8W.CompanyName = Loc.CompanyName 
    and L8W.ItemNo = Loc.ItemNo 
    and L8W.VariantCode = Loc.Variant 
    and L8W.LocationCode = Loc.LocationCode
    left join SOR on SOR.CompanyName = Loc.CompanyName
    and SOR.ItemNo = Loc.ItemNo 
    and SOR.VariantCode = Loc.Variant 
    and SOR.TransfertoCode = Loc.LocationCode
    left join WeeklySalesSum WSS on WSS.companyName = Loc.companyName
    and WSS.ItemNo = Loc.ItemNo 
    and WSS.VariantCode = Loc.Variant 
    and WSS.LocationCode = Loc.LocationCode
    left join weeklyClosingStock WCS on WCS.companyName = Loc.companyName
    and WCS.ItemNo = Loc.ItemNo 
    and WCS.VariantCode = Loc.Variant 
    and WCS.LocationCode = Loc.LocationCode 
where (
      GR.ItemNo is not null
    or PP.ItemNo is not null
    or RE.ItemNo is not NULL
    or UP.ItemNo is not null
    or LS.CostAmountTotal <> 0
    or LS.QtyOnHand <> 0
    or LS.QtyRemaining <> 0
    or (ifnull(LS.QtyOnHand, 0) - ifnull(QC.QCSOH, 0) - ifnull(T.Qty_TO, 0) - ifnull(S.Qty_SO, 0)
          + ifnull(UP.Quantity, 0) + ifnull(PP.QTY_PRO, 0) - ifnull(RE.QTY_RES, 0)
         ) <> 0
  ) ;

create or replace view FCO.GOLD.VDIMCOMPANY(
	COMPANYSKEY,
	COMPANYNAME,
	COMPANYDISPLAYNAME,
	COMPANY
) as
SELECT  CompanySKey,
       Name CompanyName,
       DisplayName CompanyDisplayName,
       case when Name = 'SDM Eyewear LIVE' then 'FCO Vision' 
        when Name = 'AGI Live' then 'AGI' 
         when Name = 'Brandmania LIVE' then 'Brandmania' 
          when Name = 'Core Merino LIVE' then 'Core Merino' 
           when Name = 'FrontierCo Wholesale LIVE' then 'FCO Wholesale' 
            when Name = 'Gazania LIVE' then 'Gazania' 
             when Name = 'Guess LIVE' then 'Guess' 
           when Name = 'MR TEKKIE LIVE' then 'MR Tekkie' 
           when Name = 'Pringle LIVE' then 'Pringle'
           
           when Name = 'Shared Services LIVE' then 'Shared Services'
           when Name = 'Travel LIVE' then 'Travel'
           when Name = 'MnG LIVE' then 'M+G'
      
           
           else Name end Company 
FROM fco.silver.dimCompany;

create or replace view FCO.GOLD.VDIMDATE(
	DIMDATEID,
	DATEDAY,
	DATEDAY_DATE,
	WEEKOFYEAR,
	DAYOFWEEKID,
	DAYOFWEEK,
	ISWEEKEND,
	DAYOFMONTH,
	DAYOFQUARTER,
	DAYOFYEAR,
	DAYCODE,
	DAYNAME,
	DAYLABEL,
	WEEKID,
	WEEKLABEL,
	WEEKOFMONTH,
	WEEKCODE,
	MONTHID,
	MONTHOFQUARTER,
	MONTHOFYEAR,
	MONTHCODE,
	MONTHNAME,
	MONTHLABEL,
	QUARTERID,
	QUARTEROFYEAR,
	QUARTERCODE,
	QUARTERNAME,
	QUARTERLABEL,
	YEARID,
	YEAR,
	YEARCODE,
	YEARNAME,
	YEARLABEL,
	FISCALDAYOFWEEK,
	FISCALWEEKID,
	FISCALWEEKLABEL,
	FISCALWEEKOFMONTH,
	FISCALWEEKCODE,
	FISCALMONTHID,
	FISCALMONTHOFQUARTER,
	FISCALMONTHOFYEAR,
	FISCALMONTHCODE,
	FISCALMONTHNAME,
	FISCALMONTHLABEL,
	FISCALQUARTERID,
	FISCALQUARTEROFYEAR,
	FISCALQUARTERCODE,
	FISCALQUARTERNAME,
	FISCALQUARTERLABEL,
	FISCALYEARID,
	FISCALYEAR,
	FISCALYEARCODE,
	FISCALYEARNAME,
	FISCALYEARLABEL,
	FINANCIALDAYOFWEEK,
	FINANCIALWEEKID,
	FINANCIALWEEKLABEL,
	FINANCIALWEEKOFMONTH,
	FINANCIALWEEKCODE,
	FINANCIALMONTHID,
	FINANCIALMONTHOFQUARTER,
	FINANCIALMONTHOFYEAR,
	FINANCIALMONTHCODE,
	FINANCIALMONTHNAME,
	FINANCIALMONTHLABEL,
	FINANCIALQUARTERID,
	FINANCIALQUARTEROFYEAR,
	FINANCIALQUARTERCODE,
	FINANCIALQUARTERNAME,
	FINANCIALQUARTERLABEL,
	FINANCIALYEARID,
	FINANCIALYEAR,
	FINANCIALYEARCODE,
	FINANCIALYEARNAME,
	FINANCIALYEARLABEL,
	RETAILDAYOFWEEK,
	TRADINGWEEK,
	TRADINGYEAR,
	TRADINGMONTHID,
	TRADINGMONTH,
	ISCURRENTDATE,
	TRADINGWEEKID,
	TRADINGWEEKLABEL,
	ISBUDGETDATE,
	DAYSINTRADINGMONTH,
	TRADINGDAYOFMONTH,
	MONTHENDDATELY,
	EOM_DIMDATEID,
	LEAPDATE,
	TODAY
) as


 

    SELECT dd.DimDateID,
        try_to_date(dd.DateDay,'DD/MM/YYYY') AS DateDay,
        try_to_date(DateDay,'DD/MM/YYYY') DateDay_Date,
        EXTRACT(WEEK from try_to_date(DateDay)) WeekOfYear,
        dd.DayOfWeekID,
        dd.DayOfWeek,
        dd.IsWeekend,
        dd.DayOfMonth,
        dd.DayOfQuarter,
        dd.DayOfYear,
        dd.DayCode,
        dd.DayName,
        dd.DayLabel,
        dd.WeekID,
        dd.WeekLabel,
        dd.WeekOfMonth,
        dd.WeekCode,
        dd.MonthID,
        dd.MonthOfQuarter,
        dd.MonthOfYear,
        dd.MonthCode,
        dd.MonthName,
        dd.MonthLabel,
        dd.QuarterID,
        dd.QuarterOfYear,
        dd.QuarterCode,
        dd.QuarterName,
        dd.QuarterLabel,
        dd.YearID,
        dd.YEAR,
        dd.YearCode,
        dd.YearName,
        dd.YearLabel,
        dd.FiscalDayOfWeek,
        dd.FiscalWeekID,
        dd.FiscalWeekLabel,
        dd.FiscalWeekOfMonth,
        dd.FiscalWeekCode,
        dd.FiscalMonthID,
        dd.FiscalMonthOfQuarter,
        dd.FiscalMonthOfYear,
        dd.FiscalMonthCode,
        dd.FiscalMonthName,
        dd.FiscalMonthLabel,
        dd.FiscalQuarterID,
        dd.FiscalQuarterOfYear,
        dd.FiscalQuarterCode,
        dd.FiscalQuarterName,
        dd.FiscalQuarterLabel,
        dd.FiscalYearID,
        dd.FiscalYear,
        dd.FiscalYearCode,
        dd.FiscalYearName,
        dd.FiscalYearLabel,
        dd.FinancialDayOfWeek,
        dd.FinancialWeekID,
        dd.FinancialWeekLabel,
        dd.FinancialWeekOfMonth,
        dd.FinancialWeekCode,
        dd.FinancialMonthID,
        dd.FinancialMonthOfQuarter,
        dd.FinancialMonthOfYear,
        dd.FinancialMonthCode,
        dd.FinancialMonthName,
        dd.FinancialMonthLabel,
        dd.FinancialQuarterID,
        dd.FinancialQuarterOfYear,
        dd.FinancialQuarterCode,
        dd.FinancialQuarterName,
        dd.FinancialQuarterLabel,
        dd.FinancialYearID,
        dd.FinancialYear,
        dd.FinancialYearCode,
        dd.FinancialYearName,
        dd.FinancialYearLabel,
        dd.RetailDayofWeek,
        dd.TradingWeek,
        dd.TradingYear,
        dd.TradingMonthID,
        dd.TradingMonth,
        dd.IsCurrentDate,
        dd.TradingWeekID,
        dd.TradingWeekLabel,
        dd.IsBudgetDate,
        COUNT(dd.DimDateID) OVER (PARTITION BY dd.TradingYear, dd.TradingMonthID) AS DaysInTradingMonth,
       
        ROW_NUMBER() OVER (PARTITION BY dd.TradingYear,
                                       dd.TradingMonthID
                          ORDER BY dd.DimDateID
                         ) AS TradingDayOfMonth,
        MEDLY.MED_LY MonthEndDateLY, MEDLY.EOM_DIMDATEID,

    0  LeapDate,
 
        case when dd.DimDateID = to_char(GETDATE(),'YYYYMMDD') then 1 else 0 end TODAY
    FROM fco.silver.DimDate AS dd  
        
        LEFT JOIN
        (
   SELECT DISTINCT
    LAST_DAY(TRY_TO_DATE(DD.DateDay)) AS MED_TY,
    LAST_DAY(TRY_TO_DATE(DDLY.DateDay)) AS MED_LY,
    DDLY.DimDateID AS EOM_DIMDATEID,
    DD.FinancialYear,
    DD.FinancialMonthOfYear
FROM fco.silver.DimDate DD
INNER JOIN fco.silver.DimDate DDLY
    ON DDLY.FinancialYear = DD.FinancialYear - 1
    AND DDLY.FinancialMonthOfYear = DD.FinancialMonthOfYear
WHERE TRY_TO_DATE(DD.DateDay) = LAST_DAY(TRY_TO_DATE(DD.DateDay)) 
  AND TRY_TO_DATE(DDLY.DateDay) = LAST_DAY(TRY_TO_DATE(DDLY.DateDay))
    ) MEDLY
        ON MEDLY.MED_TY = TRY_TO_DATE(dd.DateDay);

create or replace view FCO.GOLD.VDIMITEM(
	ITEMSKEY,
	COMPANYNAME,
	SYSTEMROWVERSION,
	NO_,
	NO_2,
	DESCRIPTION,
	SEARCHDESCRIPTION,
	DESCRIPTION2,
	BASE_UNIT_OF_MEASURE,
	PRICE_UNIT_CONVERSION,
	TYPE,
	INVENTORY_POSTING_GROUP,
	SHELF_NO_,
	ITEM_DISC_GROUP,
	ALLOW_INVOICE_DISC_,
	STATISTICS_GROUP,
	COMMISSION_GROUP,
	UNIT_PRICE,
	PRICE_PROFIT_CALCULATION,
	PROFIT_,
	COSTING_METHOD,
	UNIT_COST,
	STANDARD_COST,
	LAST_DIRECT_COST,
	INDIRECT_COST_PERC,
	COST_IS_ADJUSTED,
	ALLOW_ONLINE_ADJUSTMENT,
	VENDOR_NO,
	VENDOR_ITEM_NO,
	LEAD_TIME_CALCULATION,
	REORDER_POINT,
	MAXIMUM_INVENTORY,
	REORDER_QUANTITY,
	ALTERNATIVE_ITEM_NO_,
	UNIT_LIST_PRICE,
	DUTY_DUE_,
	DUTY_CODE,
	GROSS_WEIGHT,
	NET_WEIGHT,
	UNITS_PER_PARCEL,
	UNIT_VOLUME,
	DURABILITY,
	FREIGHT_TYPE,
	TARIFF_NO_,
	DUTY_UNIT_CONVERSION,
	COUNTRY_REGION_PURCHASED_CODE,
	BUDGET_QUANTITY,
	BUDGETED_AMOUNT,
	BUDGET_PROFIT,
	BLOCKED,
	BLOCK_REASON,
	LAST_DATETIME_MODIFIED,
	LAST_DATE_MODIFIED,
	LAST_TIME_MODIFIED,
	PRICE_INCLUDES_VAT,
	VAT_BUS_POSTING_GR_PRICE,
	GEN_PROD_POSTING_GROUP,
	PICTURE,
	COUNTRY_REGION_OF_ORIGIN_CODE,
	AUTOMATIC_EXT_TEXTS,
	NO_SERIES,
	TAX_GROUP_CODE,
	VAT_PROD_POSTING_GROUP,
	RESERVE,
	GLOBAL_DIMENSION_1_CODE,
	GLOBAL_DIMENSION_2_CODE,
	STOCKOUT_WARNING,
	PREVENT_NEGATIVE_INVENTORY,
	VARIANT_MANDATORY_IF_EXISTS,
	APPLICATION_WKSH_USER_ID,
	COUPLED_TO_CRM,
	ASSEMBLY_POLICY,
	GTIN,
	DEFAULT_DEFERRAL_TEMPLATE_CODE,
	LOW_LEVEL_CODE,
	LOT_SIZE,
	SERIAL_NOS_,
	LAST_UNIT_COST_CALC_DATE,
	ROLLED_UP_MATERIAL_COST,
	ROLLED_UP_CAPACITY_COST,
	SCRAP_,
	INVENTORY_VALUE_ZERO,
	DISCRETE_ORDER_QUANTITY,
	MINIMUM_ORDER_QUANTITY,
	MAXIMUM_ORDER_QUANTITY,
	SAFETY_STOCK_QUANTITY,
	ORDER_MULTIPLE,
	SAFETY_LEAD_TIME,
	FLUSHING_METHOD,
	REPLENISHMENT_SYSTEM,
	ROUNDING_PRECISION,
	SALES_UNIT_OF_MEASURE,
	PURCH_UNIT_OF_MEASURE,
	TIME_BUCKET,
	REORDERING_POLICY,
	INCLUDE_INVENTORY,
	MANUFACTURING_POLICY,
	RESCHEDULING_PERIOD,
	LOT_ACCUMULATION_PERIOD,
	DAMPENER_PERIOD,
	DAMPENER_QUANTITY,
	OVERFLOW_LEVEL,
	MANUFACTURER_CODE,
	ITEM_CATEGORY_CODE,
	CREATED_FROM_NONSTOCK_ITEM,
	PRODUCT_GROUP_CODE,
	PURCHASING_CODE,
	EXCLUDED_FROM_COST_ADJUSTMENT,
	EXCLUDED_FROM_PORTION_WEIGHT,
	EXCLUDED_FROM_MENU_REQUISITION,
	ITEM_TRACKING_CODE,
	LOT_NOS_,
	EXPIRATION_CALCULATION,
	WAREHOUSE_CLASS_CODE,
	SPECIAL_EQUIPMENT_CODE,
	PUT_AWAY_TEMPLATE_CODE,
	PUT_AWAY_UNIT_OF_MEASURE_CODE,
	PHYS_INVT_COUNTING_PERIOD_CODE,
	LAST_COUNTING_PERIOD_UPDATE,
	USE_CROSS_DOCKING,
	NEXT_COUNTING_START_DATE,
	NEXT_COUNTING_END_DATE,
	ID,
	UNIT_OF_MEASURE_ID,
	TAX_GROUP_ID,
	SALES_BLOCKED,
	PURCHASING_BLOCKED,
	ITEM_CATEGORY_ID,
	INVENTORY_POSTING_GROUP_ID,
	GEN_PROD_POSTING_GROUP_ID,
	SERVICE_BLOCKED,
	OVER_RECEIPT_CODE,
	ROUTING_NO_,
	PRODUCTION_BOM_NO_,
	SINGLE_LEVEL_MATERIAL_COST,
	SINGLE_LEVEL_CAPACITY_COST,
	SINGLE_LEVEL_SUBCONTRD_COST,
	SINGLE_LEVEL_CAP_OVHD_COST,
	SINGLE_LEVEL_MFG_OVHD_COST,
	OVERHEAD_RATE,
	ROLLED_UP_SUBCONTRACTED_COST,
	ROLLED_UP_MFG_OVHD_COST,
	ROLLED_UP_CAP_OVERHEAD_COST,
	ORDER_TRACKING_POLICY,
	CRITICAL,
	COMMON_ITEM_NO_,
	SERVICE_ITEM_GROUP,
	MARKDOWN_SEASON_CODE_BTR,
	ON_MARKDOWN,
	WC_PUBLISHED_BTR,
	WC_DESCRIPTION_BTR,
	WC_SHORT_DESCRIPTION_BTR,
	WC_ID_BTR,
	BRAND_CODE_BTR,
	WC_NAME_BTR,
	WC_PROCESSED_DATE_BTR,
	WC_CATEGORIES_CREATED_BTR,
	WC_CATEGORIES_CREATED_CNT_BTR,
	WC_ATTRIBUTES_CREATED_BTR,
	WC_ATTRIBUTES_CREATED_CNT_BTR,
	SYNC_TO_WOOCOM_BTR,
	ORIGINAL_RSP_BTR,
	CURRENT_ACTIVE_PRICE_BTR,
	SERVICE_COMMITMENT_OPTION,
	DATE_CREATED,
	CREATED_BY_USER,
	DIVISION_CODE,
	RETAIL_PRODUCT_CODE,
	SEASON_CODE_FIRST_PURCH_,
	LAST_MODIFIED_BY_USER,
	ITEM_ERROR_CHECK_CODE,
	ITEM_ERROR_CHECK_STATUS,
	SUGGESTED_QTY_ON_POS,
	ITEM_CAPACITY_VALUE,
	QTY_NOT_IN_DECIMAL,
	WARRANTY_CARD,
	DEF_ORDERED_BY,
	DEF_ORDERING_METHOD,
	ORIGINAL_VENDOR_NO,
	ORIGINAL_VENDOR_ITEM_NO,
	BOM_METHOD,
	BOM_RECEIPT_PRINT,
	RECIPE_VERSION_CODE,
	RECIPE_ITEM_TYPE,
	BOM_COST_PRICE_DISTRIBUTION,
	BOM_TYPE,
	BOM_RECEIVING_EXPLODE,
	EXTERNAL_ITEM_NO,
	EXTERN_SIZE_CRUST,
	VARIANT_FRAMEWORK_CODE,
	SEASON_CODE,
	LIFECYCLE_LENGTH,
	LIFECYCLE_STARTING_DATE,
	LIFECYCLE_ENDING_DATE,
	ERROR_CHECK_INTERNAL_USAGE,
	ATTRIB_1_CODE,
	ATTRIB_2_CODE,
	ATTRIB_3_CODE,
	ATTRIB_4_CODE,
	ATTRIB_5_CODE,
	ABC_SALES,
	ABC_PROFIT,
	WASTAGE_,
	EXCL_FROM_PORTION_WEIGHT,
	UNAFF_BY_MULTIPL_FACTOR,
	EXCL_FROM_MENU_REQU_,
	RECIPE_NO_OF_PORTIONS,
	MAX_MODIFIERS_NO_PRICE,
	MAX_INGR_REMOVED_NO_PRICE,
	MAX_INGR_MODIFIERS,
	PRODUCTION_TIME_MIN_,
	RECIPE_MAIN_INGREDIENT,
	RECIPE_STYLE,
	RECIPE_CATEGORY,
	AVAILABLE_AS_DISH,
	UOM_POP_UP_ON_POS,
	SHOW_UNIT_ON_POS,
	REPLEN_CALCULATION_TYPE,
	MANUAL_EST_DAILY_SALE,
	STORE_STOCK_COVER_REQD_D,
	WAREH_STOCK_COVER_REQD_D,
	REPLENISHMENT_SALES_PROF,
	REPLENISHMENT_GRADE_CODE,
	EXCLUDE_FROM_REPLENISHMENT,
	EXCLUDE_FROM_OOS_CALC_,
	TRANSFER_MULTIPLE,
	REORDER_POINT_WHSE_,
	MAXIMUM_INVENTORY_WHSE_,
	STORE_FORW_SALES_PROFILE,
	WAREH_FORW_SALES_PROFILE,
	PURCH_ORDER_DELIVERY,
	REPLENISH_AS_ITEM_NO_,
	PROFIT_GOAL_,
	TRANSF_UOM_REPLEN_,
	REPLEN_DATA_PROFILE,
	LIKE_FOR_LIKE_REPLEN_MET,
	LIKE_FOR_LIKE_PROCESS_MET,
	REPLEN_AS_ITEM_NO_MET,
	REPLEN_TRANSFER_RULE_CODE,
	SELECT_LOWEST_PRICE_VENDOR,
	EFFECTIVE_INV_SALES_ORDER,
	EFFECTIVE_INV_PURCHASE_ORD,
	EFFECTIVE_INV_TRANSFER_INB,
	EFF_INV_TRANSFER_OUTB,
	EFF_INV_ASSEMBLY_ORD_,
	EFF_INV_PURCH_RET_ORD_,
	CONFIDENCE_FACTOR_CODE,
	FORECAST_METHOD,
	FORECAST_CALC_HORIZON,
	SAFETY_STOCK_CALCULATION,
	ENABLE_LEAD_TIME_CALC,
	FORECAST_ADD_DIM_1_CODE,
	FORECAST_ADD_DIM_2_CODE,
	USE_BOM_COMPONENT_JOURNAL,
	CALC_SOLD_COMP_IN_BOM_JRNL,
	BOM_ASSEMBLED_IN,
	CONSIDER_CONSUMPT_AS_SALES,
	FUEL_ITEM,
	PLB_ITEM,
	ITEM_FAMILY_CODE,
	UNIT_PRICE_INCL_VAT,
	POS_COST_CALCULATION,
	NO_STOCK_POSTING,
	ZERO_PRICE_VALID,
	QTY_BECOMES_NEGATIVE,
	NO_DISCOUNT_ALLOWED,
	KEYING_IN_PRICE,
	SCALE_ITEM,
	KEYING_IN_QUANTITY,
	SKIP_COMPR_WHEN_SCANNED,
	SKIP_COMPR_WHEN_PRINTED,
	BARCODE_MASK,
	USE_EAN_STANDARD_BARC_,
	QTY_PER_BASE_COMP_UNIT,
	BASE_COMP_UNIT_CODE,
	COMPARISON_UNIT_CODE,
	COMP_PRICE_INCL_VAT,
	EXPLODE_BOM_IN_STATEM_POST,
	DISPENSE_PRINTER_GROUP,
	PRINT_VARIANTS_SHELF_LABEL,
	WIC_APPLICABLE,
	FOOD_STAMP_APPLICABLE,
	ASK_FOR_TARE,
	DEFAULT_TARE_WEIGHT,
	TARE_WEIGHT,
	LIFECYCLE_CURVE_CODE,
	DIMENSION_PATTERN_CODE,
	UPD_COST_AND_WEIGHT_W_POST,
	STORE_COVERAGE_DAYS_PROF_,
	WAREH_COVERAGE_DAYS_PROF,
	FORECAST_VARIANT_GROUPING,
	EXPIRY_HANDLING,
	SUST_CERT_NO_,
	SUST_CERT_NAME,
	GHG_CREDIT,
	CARBON_CREDIT_PER_UOM,
	EXCLUDE_FROM_INTRASTAT_REPORT,
	SUPPLEMENTARY_UNIT_OF_MEASURE,
	HAS_SALES_FORECAST,
	RETAIL_PRODUCT_DESCRIPTION,
	DIVISION_CODE_DESCRIPTION,
	BRAND_DESCRIPTION,
	ITEM_CATEGORY_DESCRIPTION,
	EXCLUDE_FROM_MENU_REQUISITION,
	REPLENISHMENT_CALCULATION_TYPE,
	MANUAL_ESTIMATED_DAILY_SALE,
	STORE_STOCK_COVER_REQD_DAYS,
	WAREH_STOCK_COVER_REQD_DAYS,
	REPLENISHMENT_SALES_PROFILE,
	STORE_FORWARD_SALES_PROFILE,
	WAREH_FORWARD_SALES_PROFILE,
	LIKE_FOR_LIKE_REPLEN_METHOD,
	LIKE_FOR_LIKE_PROCESS_METHOD,
	REPLENISH_AS_ITEM_NO_METHOD,
	EFFECTIVE_INV_PURCHASE_ORD_,
	EFFECTIVE_INV_TRANSFER_INB_,
	EFFECTIVE_INV_TRANSFER_OUTB_,
	LS_FORECAST_CALC_HORIZON,
	ENABLE_LEAD_TIME_CALCULATION,
	SKIP_COMPRESSION_WHEN_SCANNED,
	SKIP_COMPRESSION_WHEN_PRINTED,
	PRINT_VARIANTS_SHELF_LABELS,
	STORE_COVERAGE_DAYS_PROFILE,
	WAREH_COVERAGE_DAYS_PROFILE,
	SYSTEMID,
	SYSTEMCREATEDAT,
	SYSTEMCREATEDBY,
	SYSTEMMODIFIEDAT,
	SYSTEMMODIFIEDBY
) as
                              select *  from fco.silver.dimitem;

create or replace view FCO.GOLD.VDIMITEMVARIANT(
	ITEMVARIANTSKEY,
	COMPANYNAME,
	ITEMNO,
	VARIANT_DIMENSION_1,
	VARIANT_DIMENSION_2,
	VARIANT_DIMENSION_3,
	VARIANT_DIMENSION_4,
	VARIANT_DIMENSION_5,
	VARIANT_DIMENSION_6,
	FRAMEWORKCODE,
	VARIANT,
	BARCODE,
	FUELPRICEGROUPID,
	LOGICAL_ORDER,
	DIMENSION_WEIGHT_1,
	DIMENSION_WEIGHT_2,
	DIMENSION_WEIGHT_3,
	DIMENSION_WEIGHT_4,
	DIMENSION_WEIGHT_5,
	DIMENSION_WEIGHT_6,
	VARIANTWEIGHT,
	DESCRIPTION,
	DESCRIPTION_2,
	WC_UN_PUBLISHED_BTR,
	WC_ENTITY_EXIST,
	WC_DESCRIPTION_BTR,
	WC_ID_BTR,
	WC_ATTRIBUTES_CREATED_BTR,
	WC_ATTRIBUTES_CREATED_CNT_BTR,
	WC_PARENT_ID_BTR,
	ITEMVARIANTUNITPRICE,
	OPTIONONMD
) as

with PRICV
        as
        (
            SELECT SPI.CompanyName,
                SPI.itemNo ,
                SPI.variantCode,
                SPI.startingDate,
                SPI.endingDate ,
                SPI.unitPrice , SPI.onMarkdown OnMD ,
                ROW_NUMBER() OVER (PARTITION BY SPI.CompanyName, SPI.itemNo,
                SPI.variantCode 
                              ORDER BY SPI.CompanyName,SPI.itemNo,
                SPI.variantCode,
                SPI.startingDate  DESC
                             ) AS RN
            from fco.silver.factSalesPrice SPI 
     
            where
                     try_to_date(startingDate) < getdate()
                AND SPI.salesCode = 'ALL'
                AND
                (
                   try_to_date(endingDate) > GETDATE()
                OR try_to_date(endingDate) is null 
               )
            --  and [Unit of Measure Code] = 'EACH'  

        ) ,
        PRIC
        as
        (
               SELECT SPI.CompanyName,
                SPI.itemNo  ,
                SPI.variantCode  ,
                SPI.startingDate  ,
                SPI.endingDate  ,
                SPI.unitPrice , SPI.onMarkdown OnMD ,
                ROW_NUMBER() OVER (PARTITION BY SPI.CompanyName, SPI.itemNo,
                SPI.variantCode 
                              ORDER BY SPI.CompanyName,SPI.itemNo,
                SPI.variantCode,
                SPI.startingDate  DESC
                             ) AS RN
            from fco.silver.factSalesPrice SPI  
     
            where
                     try_to_date( startingDate) < getdate()
                AND SPI.salesCode = 'ALL'
                AND
                (
                   try_to_date(endingDate) > GETDATE()
                OR try_to_date(endingDate) is null 
               )
                and SPI.variantCode is null

        )


    SELECT --ROW_NUMBER() OVER (ORDER BY CompanyName, [Item No_], Variant) ItemVariantSKey
        ItemVariantSKey, IV.CompanyName,
        IV.Item_No_ ItemNo ,
        Variant_Dimension_1 ,
        Variant_Dimension_2 ,
        Variant_Dimension_3 ,
        Variant_Dimension_4 ,
        Variant_Dimension_5 ,
        Variant_Dimension_6 ,
        '' FrameworkCode,
        Variant,
        Barcode,
        '' FuelPriceGroupId,
        Logical_Order ,
        Dimension_Weight_1 ,
        Dimension_Weight_2 ,
        Dimension_Weight_3 ,
        Dimension_Weight_4 ,
        Dimension_Weight_5 ,
        Dimension_Weight_6 ,
        '' VariantWeight,
        Description,
        Description_2 ,
        WC_Un_Published_BTR ,
        WC_Entity_Exist  ,
        WC_Description_BTR  ,
        WC_Id_BTR ,
        WC_Attributes_Created_BTR  ,
        WC_Attributes_Created_Cnt_BTR  ,
        WC_Parent_Id_BTR  , COALESCE(PV.UnitPrice, P.UnitPrice) ItemVariantUnitPrice,
        case when ifnull(COALESCE( PV.OnMD ,  P.OnMD ),'FALSE') = 'TRUE' then 'Y' else 'N' end  OptionOnMD
    FROM fco.silver.dimItemVariant IV 
    left join PRICV PV on PV.CompanyName = IV.CompanyName
    and PV.itemNo    = IV.Item_No_
    and PV.VariantCode = IV.Variant
    and PV.RN = 1
    left join PRIC P on P.CompanyName = IV.CompanyName and P.ItemNo =  IV.Item_No_ and P.RN = 1;

create or replace view FCO.GOLD.VDIMLOCATION(
	LOCATIONSKEY,
	COMPANYID,
	STOREID,
	LOCATIONCODE,
	LOCATIONNAME,
	COMPANYNAME,
	USE_AS_IN_TRANSIT,
	REQUIRE_PUTAWAY,
	GIT_LOCATION,
	LBY_LOCATION,
	REQUIRE_PICK,
	USE_CROSSDOCKING,
	REQUIRE_RECEIVE,
	REQUIRE_SHIPMENT,
	BIN_MANDATORY,
	DIRECTEDPUTAWAY_PICK,
	LOCATIONRECEIPTBIN,
	LOCATIONSHIPMENTBIN,
	USEADCS,
	LOCATION_IS_A_WAREHOUSE,
	LOCATIONSTATUS
) as
            SELECT -9 LocationSKey, -9 AS   CompanyID,
            -9 AS   StoreID, 'UnKnown' AS   LocationCode,
            'UnKnown' AS    LocationName,
            'UnKnown' AS CompanyName ,
            NULL AS   Use_As_In_Transit,
            NULL AS   Require_PutAway,
            'UnKnown' AS   GIT_Location,
            'UnKnown' AS   LBY_Location,
            NULL AS   Require_Pick,
            NULL AS   Use_CrossDocking,
            NULL AS   Require_Receive,
            NULL AS   Require_Shipment,
            NULL AS   Bin_Mandatory,
            NULL AS   DirectedPutAway_Pick,
            'UnKnown' AS  LocationReceiptBin,
            'UnKnown' AS  LocationShipmentBin,
            'UnKnown' AS   UseADCS,
            'UnKnown' AS    Location_is_a_Warehouse,
            'UnKnown' AS   LocationStatus

    UNION ALL

SELECT  DL.LocationSKey,
         DSL.CompanySKey CompanyID,
            DS.StoreSKey StoreID, DL.Code LocationCode,
            DL.Name LocationName,
            DL.CompanyName,
            DL.Use_As_In_Transit Use_As_In_Transit,
            DL.Require_Put_away Require_PutAway,
            CASE WHEN DL.Use_As_In_Transit = 'False' THEN 'NO' WHEN DL.Use_As_In_Transit = 'True' THEN 'YES'END GIT_Location,
            CASE WHEN DL.Code LIKE '%LBY' THEN 'YES' ELSE  'NO' END LBY_Location,
            DL.Require_Pick Require_Pick,
            DL.Use_Cross_Docking Use_CrossDocking,
            DL.Require_Receive Require_Receive,
            DL.Require_Shipment Require_Shipment,
            DL.Bin_Mandatory Bin_Mandatory,
            DL.Directed_Put_away_and_Pick DirectedPutAway_Pick,
            DL.Receipt_Bin_Code LocationReceiptBin,
            DL.Shipment_Bin_Code LocationShipmentBin,
            CASE WHEN DL.Use_ADCS = 'False' THEN 'NO' WHEN DL.Use_ADCS = 'True' THEN 'YES' END UseADCS,
            CASE WHEN DL.Location_is_a_Warehouse = 'False' THEN 'No' WHEN DL.Location_is_a_Warehouse = 'True' THEN 'YES' END  Location_is_a_Warehouse,
            CASE WHEN DL.Closed = 'True' THEN 'CLOSED' WHEN DL.Closed = 'False' THEN 'OPEN' END LocationStatus
        FROM (
        
        SELECT DC.CompanySKey,
                SL.storeNo,
                SL.LocationCode, SL.CompanyName
            FROM fco.gold.vDimStoreLocations SL 
            LEFT JOIN fco.gold.vDimCompany DC ON DC.CompanyName = SL.CompanyName 
            
            ) 
            as DSL 
            LEFT outer JOIN fco.gold.vdimStore DS ON DS.companyid = DSL.companyskey
                AND DS.StoreNo = DSL.storeNo
            LEFT JOIN fco.silver.dimLocation DL ON DL.CompanyName = DSL.CompanyName AND DL.Code = DSL.LocationCode
            where DL.LocationSKey <> 684;

create or replace view FCO.GOLD.VDIMSTORE(
	STORESKEY,
	COMPANYID,
	COMPANYNAME,
	STORENO,
	STORENAME,
	STORE_ADDRESS,
	STORE_ADDRESS2,
	STORECITY,
	STORE_POSTALCODE,
	STORE_PHONE,
	STORE_COUNTRY,
	GLOBALDIM1,
	STOREDFLTLOCATION,
	GLOBALDIM2,
	STORETYPE,
	LATITUDE,
	LONGITUDE,
	STOREATTRIB_1,
	STOREATTRIB_2,
	STOREATTRIB_3,
	STOREATTRIB_4,
	STOREATTRIB_5,
	GITCODE,
	NUMBEROFPOSTERMINALS,
	STORESTATUS,
	STOREXREF
) as 

SELECT  * FROM (
SELECT  S.StoreSKey,
       DC.CompanySKey CompanyID,
	   DC.name CompanyName,
       S.No_ StoreNo,
       S.Name StoreName,
       S.Address Store_Address,
       S.Address_2 Store_Address2,
       S.City StoreCity,
       S.Post_Code Store_PostalCode,
       S.Phone_No_ Store_Phone,
       S.Country_Code Store_Country,
       S.Global_Dimension_1_Code GlobalDim1,
       S.Location_Code StoreDfltLocation,
       S.Global_Dimension_2_Code GlobalDim2,
       S.Store_Type StoreType,
       S.Latitude Latitude,
       S.Longitude Longitude,
       S.Attrib_1_Code StoreAttrib_1,
       S.Attrib_2_Code StoreAttrib_2,
       S.Attrib_3_Code StoreAttrib_3,
       S.Attrib_4_Code StoreAttrib_4,
       S.Attrib_5_Code StoreAttrib_5,
       S.In_Transit_Code GITCode,
       S.Number_of_POS_terminals NumberofPOSterminals,
       CASE WHEN S.Closed_BTR = 'True' THEN 'CLOSED' WHEN S.Closed_BTR = 'False' THEN 'OPEN' END StoreStatus,
       S.Store_XRef_No_BTR StoreXRef
FROM   FCO.SILVER.DIMSTORE S
    LEFT JOIN FCO.SILVER.DIMCOMPANY DC
        ON DC.name = S.CompanyName  
        ) A
        UNION ALL
SELECT 
       -9, -9 CompanySKey,'' CompanyName,'' StoreNo,
       'UnKnown' StoreName,
	     
       '' Store_Address,
       '' Store_Address2,
       '' StoreCity,
       '' Store_PostalCode,
       '' Store_Phone,
       '' Store_Country,
       '' GlobalDim1,
       '' StoreDfltLocation,
       '' GlobalDim2,
       'STORE' StoreType,
       0 Latitude,
       0  Longitude,
       ''  Attrib1Code  ,
       ''  Attrib2Code  ,
       ''  Attrib3Code  ,
       ''  Attrib4Code  ,
       ''  Attrib5Code  ,
       ''  InTransitCode  ,
       0  NumberofPOSterminals,
       'OPEN'  Closed_BTR   ,
       ''  StoreXRefNo_BTR ;

create or replace view FCO.GOLD.VDIMSTORELOCATIONS(
	COMPANYNAME,
	STORENO,
	LOCATIONCODE
) as
select companyName as CompanyName,
Store_No_ as StoreNo,
Location_Code LocationCode from fco.silver.dimStoreLocation;

create or replace view FCO.GOLD.VFACTITEMLEDGERENTRY(
	COMPANYID,
	ITEMID,
	ITEMVARIANTID,
	LOCATIONID,
	COMPANYNAME,
	ENTRYNO,
	ITEMNO,
	POSTINGDATEID,
	ENTRYTYPEDESC,
	ENTRYTYPE,
	DOCUMENTTYPEID,
	SOURCENO,
	DOCUMENTNO,
	DESCRIPTION,
	LOCATIONCODE,
	QUANTITY,
	REMAININGQUANTITY,
	INVOICEDQUANTITY,
	APPLIESTOENTRY,
	OPEN,
	GLOBALDIMENSION1CODE,
	GLOBALDIMENSION2CODE,
	POSITIVE,
	SHPTMETHODCODE,
	SOURCETYPE,
	DROPSHIPMENT,
	TRANSACTIONTYPE,
	TRANSPORTMETHOD,
	COUNTRYREGIONCODE,
	ENTRYEXITPOINT,
	DOCUMENTDATE,
	EXTERNALDOCUMENTNO,
	AREA,
	TRANSACTIONSPECIFICATION,
	NOSERIES,
	DOCUMENTTYPEDESC,
	DOCUMENTLINENO,
	ORDERTYPE,
	ORDERNO,
	ORDERLINENO,
	DIMENSIONSETID,
	ASSEMBLETOORDER,
	JOBNO,
	JOBTASKNO,
	JOBPURCHASE,
	VARIANTCODE,
	QTYPERUNITOFMEASURE,
	UNITOFMEASURECODE,
	DERIVEDFROMBLANKETORDER,
	CROSSREFERENCENO_,
	ORIGINALLYORDEREDNO,
	ORIGINALLYORDEREDVARCODE,
	OUTOFSTOCKSUBSTITUTION,
	ITEMCATEGORYCODE,
	NONSTOCK,
	PURCHASINGCODE,
	LSCRETAILPRODUCTCODE,
	ITEMREFERENCENO,
	COMPLETELYINVOICED,
	LASTINVOICEDATE,
	APPLIEDENTRYTOADJUST,
	CORRECTION,
	SHIPPEDQTYNOTRETURNED,
	PRODORDERCOMPLINENO,
	SERIALNO,
	LOTNO,
	WARRANTYDATE,
	EXPIRATIONDATE,
	ITEMTRACKING,
	RETURNREASONCODE,
	COSTAMOUNTEXPECTED,
	COSTAMOUNTACTUAL,
	COSTAMOUNTACTUALNETT
) as 

SELECT IFNULL(DC.CompanySKey, -9) CompanyID,
       IFNULL(DI.ItemSKey, -9) ItemID,
       IFNULL(IV.ItemVariantSKey, -9) ItemVariantID,
       IFNULL(DL.LocationSKey, -9) LocationID,
       ILE.CompanyName,
       ILE.entryNo  ,
       ILE.itemNo  ,
       
       to_char(try_to_date(ILE.postingDate),'YYYYMMDD') PostingDateID,
       ILE.entrytype entryTypeDesc ,
       CASE
           WHEN entryType = 'Purchase' THEN
               0
           WHEN entryType = 'Sale' THEN
               1
           WHEN entryType =  'Positive Adjustment' THEN
              2
           WHEN entryType =  'Negative Adjustment'  THEN
              3
           WHEN entryType = 'Transfer'  THEN
               4
           WHEN entryType =  'Consumption' THEN
               5
           WHEN entryType =  'Output'  THEN
               6
       END AS entryType,
       CASE
           WHEN documentType = ''  THEN
               0
           WHEN documentType = 'Sales Shipment' THEN
               1
           WHEN documentType = 'Sales Invoice'  THEN
               2
           WHEN documentType = 'Sales Return Receipt'  THEN
               3
           WHEN documentType = 'Sales Credit Memo' THEN
               4
           WHEN documentType = 'Purchase Receipt'  THEN
               5
           WHEN documentType = 'Purchase Invoice'  THEN
               6
           WHEN documentType = 'Purchase Return Shipment'  THEN
               7
           WHEN documentType = 'Purchase Credit Memo'  THEN
              8 
           WHEN documentType = 'Transfer Shipment'  THEN
               9
           WHEN documentType = 'Transfer Receipt'  THEN
              10 
           WHEN documentType = 'Service Shipment'  THEN
               11
           WHEN documentType = 'Service Invoice'  THEN
               12
           WHEN documentType = 'Service Credit Memo'  THEN
               13
           WHEN documentType = 'Posted Assembly'  THEN
               14
           WHEN documentType = 'Inventory Receipt'  THEN
               15
           WHEN documentType = 'Inventory Shipment'  THEN
               16
           WHEN documentType = 'Direct Transfer'  THEN
               17
       END AS DocumentTypeID,
       ILE.sourceNo  ,
       ILE.documentNo  , 
       ILE.Description,
       ILE.locationCode  ,
       ILE.Quantity,
       ILE.remainingQuantity  ,
       ILE.invoicedQuantity  ,
       ILE.appliesToEntry  ,
       ILE.Open,
       ILE.globalDimension1Code  ,
       ILE.globalDimension2Code  ,
       ILE.Positive,
       ILE.shptMethodCode  ,
       ILE.sourceType  ,
       ILE.dropShipment  ,
       ILE.transactionType  ,
       ILE.transportMethod  ,
       ILE.countryRegionCode  ,
       ILE.entryExitPoint  ,
       ILE.documentDate  ,
       ILE.externalDocumentNo  ,
       ILE.Area,
       ILE.transactionSpecification  ,
       ILE.noSeries  ,
       ILE.documentType documentTypeDesc  ,
       ILE.documentLineNo  ,
       ILE.orderType  ,
       ILE.orderNo  ,
       ILE.orderLineNo  ,
       ILE.dimensionSetID  ,
       ILE.assembleToOrder  ,
       ILE.jobNo  ,
       ILE.jobTaskNo  ,
       ILE.jobPurchase  ,
       ILE.variantCode  ,
       ILE.qtyPerUnitOfMeasure  ,
       ILE.unitOfMeasureCode  ,
       ILE.derivedFromBlanketOrder  ,
       ILE.crossreferenceno_,
       ILE.originallyOrderedNo  ,
       ILE.originallyOrderedVarCode  ,
       ILE.outOfStockSubstitution  ,
       ILE.itemCategoryCode  ,
       ILE.Nonstock,
       ILE.purchasingCode  ,
       ILE.lscRetailProductCode  ,
       ILE.itemReferenceNo  ,
       ILE.completelyInvoiced  ,
       try_to_date(lastInvoiceDate) lastInvoiceDate,
       ILE.appliedEntryToAdjust  ,
       ILE.Correction,
       ILE.shippedQtyNotReturned  ,
       ILE.prodOrderCompLineNo  ,
       ILE.serialNo  ,
       ILE.lotNo  ,
       try_to_date(warrantyDate) warrantyDate,
       try_to_date(expirationDate) expirationDate,
       ILE.itemTracking  ,
       ILE.returnReasonCode  , 
       VE.costamountexpected,
       VE.costamountactual,
       VE.costamountactual + VE.costamountexpected CostAmountActualNett
  
      -- coalesce(spv.[Unit Price],sp.[Unit Price], DI.[Unit Price]) [Unit Price Final],
      --coalesce(spv.[Unit Price],sp.[Unit Price], DI.[Unit Price],0) * ILE.invoicedQuantity  InvatRSPInc 
FROM fco.silver.factItemLedgerEntry ILE
    LEFT JOIN fco.gold.VFACTVALUEENTRY_ILE VE
        ON VE.CompanyName = ILE.CompanyName
           AND VE.itemNo = ILE.itemNo
           AND VE.variantCode = ILE.variantCode
           AND VE.itemLedgerEntryNo = ILE.entryNo

 
    LEFT JOIN fco.gold.vDimCompany DC
        ON DC.CompanyName = ILE.CompanyName  
    LEFT JOIN fco.gold.vDimItem DI
        ON DI.CompanyName = ILE.CompanyName
           AND DI.NO_ = ILE.itemNo
    LEFT JOIN fco.gold.vDimLocation DL
        ON DL.CompanyName = ILE.CompanyName
           AND DL.LocationCode = ILE.locationCode
    LEFT JOIN fco.gold.vDimItemVariant IV
        ON IV.CompanyName = ILE.CompanyName
           AND IV.ITEMNO = ILE.itemNo
           AND IV.Variant = ILE.variantCode;

create or replace view FCO.GOLD.VFACTSALESCREDITMEMO(
	COMPANYNAME,
	DOCUMENTNO_,
	SELLTOCUSTOMERNO,
	BILLTOCUSTOMERNO,
	BILLTONAME,
	BILLTONAME2,
	BILLTOADDRESS,
	BILLTOADDRESS2,
	BILLTOCITY,
	BILLTOCONTACT,
	YOURREFERENCE,
	SHIPTOCODE,
	SHIPTONAME,
	SHIPTONAME2,
	SHIPTOADDRESS,
	SHIPTOADDRESS2,
	SHIPTOCITY,
	SHIPTOCONTACT,
	POSTINGDATE,
	SHIPMENTDATE,
	POSTINGDESCRIPTION,
	PAYMENTTERMSCODE,
	DUEDATE,
	PAYMENTDISCOUNT,
	PMT_DISCOUNTDATE,
	SHIPMENTMETHODCODE,
	LOCATIONCODE,
	SHORTCUTDIMENSION1CODE,
	SHORTCUTDIMENSION2CODE,
	CUSTOMERPOSTINGGROUP,
	CURRENCYCODE,
	CURRENCYFACTOR,
	CUSTOMERPRICEGROUP,
	PRICESINCLUDINGVAT,
	INVOICEDISCCODE,
	CUSTOMERDISCGROUP,
	LANGUAGECODE,
	SALESPERSONCODE,
	NOPRINTED,
	ONHOLD,
	APPLIESTODOCTYPE,
	APPLIESTODOCNO,
	BALACCOUNTNO,
	VATREGISTRATIONNO,
	REASONCODE,
	GENBUSPOSTINGGROUP,
	EU3PARTYTRADE,
	TRANSACTIONTYPE,
	TRANSPORTMETHOD,
	VATCOUNTRYREGIONCODE,
	SELLTOCUSTOMERNAME,
	SELLTOCUSTOMERNAME2,
	SELLTOADDRESS,
	SELLTOADDRESS2,
	SELLTOCITY,
	SELLTOCONTACT,
	BILLTOPOSTCODE,
	BILLTOCOUNTY,
	BILLTOCOUNTRYREGIONCODE,
	SELLTOPOSTCODE,
	SELLTOCOUNTY,
	SELLTOCOUNTRYREGIONCODE,
	SHIPTOPOSTCODE,
	SHIPTOCOUNTY,
	SHIPTOCOUNTRYREGIONCODE,
	BALACCOUNTTYPE,
	EXITPOINT,
	CORRECTION,
	DOCUMENTDATE,
	EXTERNALDOCUMENTNO,
	AREA,
	TRANSACTIONSPECIFICATION,
	PAYMENTMETHODCODE,
	SHIPPINGAGENTCODE,
	PACKAGETRACKINGNO,
	PREASSIGNEDNOSERIES,
	NOSERIES,
	PREASSIGNEDNO,
	USERID,
	SOURCECODE,
	TAXAREACODE,
	TAXLIABLE,
	VATBUSPOSTINGGROUP,
	VATBASEDISCOUNT,
	PREPMTCRMEMONOSERIES,
	PREPAYMENTCREDITMEMO,
	PREPAYMENTORDERNO,
	SELLTOPHONENO,
	SELLTOEMAIL,
	WORKDESCRIPTION,
	DIMENSIONSETID,
	DOCUMENTEXCHANGESTATUS,
	DOCEXCHORIGINALIDENTIFIER,
	CUSTLEDGERENTRYNO,
	CAMPAIGNNO,
	SELLTOCONTACTNO,
	BILLTOCONTACTNO,
	OPPORTUNITYNO,
	RESPONSIBILITYCENTER,
	SHIPPINGAGENTSERVICECODE,
	RETURNORDERNO,
	RETURNORDERNOSERIES,
	PRICECALCULATIONMETHOD,
	ALLOWLINEDISC,
	GETRETURNRECEIPTUSED,
	ID,
	DRAFTCRMEMOSYSTEMID,
	FORMATREGION,
	COMPANYBANKACCOUNTCODE,
	VATREPORTINGDATE,
	RCVDFROMCOUNTREGIONCODE,
	LINENO,
	TYPE,
	ITEMNO,
	POSTINGGROUP,
	DESCRIPTION,
	DESCRIPTION2,
	STORECODE,
	UNITOFMEASURE,
	QUANTITY,
	UNITPRICE,
	UNITCOSTLCY,
	VAT,
	LINEDISCOUNT,
	LINEDISCOUNTAMOUNT,
	AMOUNT,
	AMOUNTINCLUDINGVAT,
	ALLOWINVOICEDISC,
	GROSSWEIGHT,
	NETWEIGHT,
	UNITSPERPARCEL,
	UNITVOLUME,
	APPLTOITEMENTRY,
	JOBNO,
	WORKTYPECODE,
	ORDERNO,
	ORDERLINENO,
	INVDISCOUNTAMOUNT,
	VATCALCULATIONTYPE,
	ATTACHEDTOLINENO,
	TAXCATEGORY,
	TAXGROUPCODE,
	VATCLAUSECODE,
	BLANKETORDERNO,
	BLANKETORDERLINENO,
	VATBASEAMOUNT,
	UNITCOST,
	SYSTEMCREATEDENTRY,
	LINEAMOUNT,
	VATDIFFERENCE,
	VATIDENTIFIER,
	ICPARTNERREFTYPE,
	ICPARTNERREFERENCE,
	PREPAYMENTLINE,
	ICPARTNERCODE,
	ICITEMREFERENCENO,
	PMTDISCOUNTAMOUNT,
	LINEDISCOUNTCALCULATION,
	JOBTASKNO,
	JOBCONTRACTENTRYNO,
	DEFERRALCODE,
	VARIANTCODE,
	BINCODE,
	QTYPERUNITOFMEASURE,
	UNITOFMEASURECODE,
	QUANTITYBASE,
	FAPOSTINGDATE,
	DEPRECIATIONBOOKCODE,
	DEPRUNTILFAPOSTINGDATE,
	DUPLICATEINDEPRECIATIONBOOK,
	USEDUPLICATIONLIST,
	CROSSREFERENCENO,
	UNITOFMEASURECROSSREF,
	CROSSREFERENCETYPE,
	CROSSREFERENCETYPENO,
	ITEMCATEGORYCODE,
	NONSTOCK,
	PURCHASINGCODE,
	PRODUCTGROUPCODE,
	ITEMREFERENCENO,
	ITEMREFERENCEUNITOFMEASURE,
	ITEMREFERENCETYPE,
	ITEMREFERENCETYPENO,
	APPLFROMITEMENTRY,
	RETURNRECEIPTNO,
	RETURNRECEIPTLINENO,
	RETURNREASONCODE
) as
SELECT CM.CompanyName,
       CM.no DocumentNo_ ,
       CM.sellToCustomerNo  ,
       CM.billToCustomerNo  ,
       CM.billToName  ,
       CM.billToName2  ,
       CM.billToAddress  ,
       CM.billToAddress2  ,
       CM.billToCity  ,
       CM.billToContact  ,
       CM.yourReference  ,
       CM.shipToCode  ,
       CM.shipToName  ,
       CM.shipToName2  ,
       CM.shipToAddress  ,
       CM.shipToAddress2  ,
       CM.shipToCity  ,
       CM.shipToContact  ,
       try_to_date( CM.postingDate) PostingDate,
       try_to_date( CM.shipmentDate ) ShipmentDate,
       CM.postingDescription  ,
       CM.paymentTermsCode  ,
       try_to_date(CM.dueDate ) DueDate,
       CM.paymentDiscount ,
       try_to_date(CM.pmtDiscountDate ) Pmt_DiscountDate,
       CM.shipmentMethodCode ,
         case when ML.locationCode = '' then  DL.LocationCode else ML.locationCode end  LocationCode ,
       CM.shortcutDimension1Code  ,
       CM.shortcutDimension2Code ,
       CM.customerPostingGroup  ,
       CM.currencyCode  ,
       CM.currencyFactor  ,
       CM.customerPriceGroup  ,
       CM.pricesIncludingVAT  ,
       CM.invoiceDiscCode  ,
       CM.customerDiscGroup  ,
       CM.languageCode  ,
       CM.salespersonCode  ,
       CM.noPrinted  ,
       CM.onHold  ,
       CM.appliesToDocType  ,
       CM.appliesToDocNo  ,
       CM.balAccountNo  ,
       CM.vatRegistrationNo  ,
       CM.reasonCode  ,
       CM.genBusPostingGroup  ,
       CM.eu3PartyTrade  ,
       CM.transactionType  ,
       CM.transportMethod  ,
       CM.vatCountryRegionCode  ,
       CM.sellToCustomerName  ,
       CM.sellToCustomerName2  ,
       CM.sellToAddress  ,
       CM.sellToAddress2  ,
       CM.sellToCity  ,
       CM.sellToContact  ,
       CM.billToPostCode  ,
       CM.billToCounty  ,
       CM.billToCountryRegionCode  ,
       CM.sellToPostCode  ,
       CM.sellToCounty  ,
       CM.sellToCountryRegionCode  ,
       CM.shipToPostCode  ,
       CM.shipToCounty  ,
       CM.shipToCountryRegionCode  ,
       CM.balAccountType  ,
       CM.exitPoint  ,
       CM.correction  ,
       try_to_date(CM.documentDate ) DocumentDate,
       CM.externalDocumentNo ,
       CM.area  ,
       CM.transactionSpecification  ,
       CM.paymentMethodCode  ,
       CM.shippingAgentCode  ,
       CM.packageTrackingNo  ,
       CM.preAssignedNoSeries  ,
       CM.noSeries  ,
       CM.preAssignedNo  ,
       CM.userID  ,
       CM.sourceCode  ,
       CM.taxAreaCode  ,
       CM.taxLiable  ,
       CM.vatBusPostingGroup  ,
       CM.vatBaseDiscount  ,
       CM.prepmtCrMemoNoSeries  ,
       CM.prepaymentCreditMemo  ,
       CM.prepaymentOrderNo  ,
       CM.sellToPhoneNo ,
       CM.sellToEMail  ,
       CM.workDescription  ,
       CM.dimensionSetID  , 
       CM.documentExchangeStatus  ,
       CM.docExchOriginalIdentifier   ,
       CM.custLedgerEntryNo  ,
       CM.campaignNo  ,
       CM.sellToContactNo  ,
       CM.billToContactNo  ,
       CM.opportunityNo  ,
       CM.responsibilityCenter  ,
       CM.shippingAgentServiceCode  ,
       CM.returnOrderNo  ,
       CM.returnOrderNoSeries  ,
       CM.priceCalculationMethod  ,
       CM.allowLineDisc  ,
       CM.getReturnReceiptUsed  ,
       CM.id  ,
       CM.draftCrMemoSystemId  , 
       CM.formatRegion  ,
       CM.companyBankAccountCode  ,
       try_to_date(CM.vatReportingDate ) vatReportingDate ,
       CM.rcvdFromCountRegionCode  ,
       ML.lineNo  , 
       ML.type ,
       ML.no ItemNo , 
       ML.postingGroup , 
       ML.description ,
       ML.description2 ,
       ML.shortcutDimension1Code StoreCode,
       ML.unitOfMeasure ,
       ML.quantity ,
       ML.unitPrice ,
       ML.unitCostLCY ,
       ML.vat ,
       ML.lineDiscount ,
       ML.lineDiscountAmount ,
       ML.amount Amount,
       ML.amountIncludingVAT ,
       ML.allowInvoiceDisc ,
       ML.grossWeight ,
       ML.netWeight ,
       ML.unitsPerParcel ,
       ML.unitVolume ,
       ML.applToItemEntry ,  
       ML.jobNo ,
       ML.workTypeCode ,
       ML.orderNo ,
       ML.orderLineNo , 
       ML.invDiscountAmount , 
       ML.vatCalculationType ,  
       ML.attachedToLineNo ,   
       ML.taxCategory ,  
       ML.taxGroupCode ,
       ML.vatClauseCode , 
       ML.blanketOrderNo ,
       ML.blanketOrderLineNo ,
       ML.vatBaseAmount ,
       ML.unitCost ,
       ML.systemCreatedEntry ,
       ML.lineAmount ,
       ML.vatDifference ,
       ML.vatIdentifier ,
       ML.icPartnerRefType ,
       ML.icPartnerReference ,
       ML.prepaymentLine ,
       ML.icPartnerCode , 
       ML.icItemReferenceNo ,
       ML.pmtDiscountAmount ,
       ML.lineDiscountCalculation , 
       ML.jobTaskNo ,
       ML.jobContractEntryNo ,
       ML.deferralCode ,
       ML.variantCode ,
       ML.binCode ,
       ML.qtyPerUnitOfMeasure ,
       ML.unitOfMeasureCode ,
       ML.quantityBase ,
       try_to_date(ML.faPostingDate ) faPostingDate,
       ML.depreciationBookCode ,
       try_to_date(ML.deprUntilFAPostingDate ) deprUntilFAPostingDate,
       ML.duplicateInDepreciationBook ,
       ML.useDuplicationList , 
       ML.crossReferenceNo ,
       ML.unitOfMeasureCrossRef ,
       ML.crossReferenceType ,
       ML.crossReferenceTypeNo ,
       ML.itemCategoryCode ,
       ML.nonstock ,
       ML.purchasingCode ,
       ML.productGroupCode ,
       ML.itemReferenceNo ,
       ML.itemReferenceUnitOfMeasure ,
       ML.itemReferenceType ,
       ML.itemReferenceTypeNo ,
       ML.applFromItemEntry ,
       ML.returnReceiptNo ,
       ML.returnReceiptLineNo ,
       ML.returnReasonCode    FROM fco.silver.factSalesCr_MemoHeader CM
LEFT JOIN fco.silver.factSalesCr_MemoLine ML ON ML.CompanyName = CM.CompanyName AND ML.documentNo = CM.no
left join fco.gold.vdimStore DS on DS.CompanyName   = ML.CompanyName and DS.StoreNo   = ML.shortcutDimension1Code
 left join fco.gold.vDimLocation DL on DL.LocationCode    = DS.StoreDfltLocation  
  and DL.CompanyName    = DS.CompanyName  ;

create or replace view FCO.GOLD.VFACTSALESDETAIL(
	STORESKEY,
	SALESDATE,
	QTYSOLD,
	NETAMT
) as 

select L.storeskey, TO_DATE(DATE) SalesDate , sum(quantity * -1)  QtySold, sum(netamount) *-1 NetAmt
from fco.silver.factlsctrans_salesentry S 
left join fco.gold.vdimstore L on S.companyname = L.companyname and S.storeno = L.storeno
where L.storeno = '5600'
group by L.storeskey, TO_DATE(DATE)  
;

create or replace view FCO.GOLD.VFACTSALESDETAIL_V1(
	SALESDATE_V1,
	NETAMT,
	STORESKEY
) as SELECT
    to_timestamp_ntz(SALESDATE) as SALESDATE_v1,
    NETAMT,
    STORESKEY
FROM VFACTSALESDETAIL;

create or replace view FCO.GOLD.VFACTVALUEENTRY_ILE(
	COMPANYNAME,
	ITEMNO,
	LOCATIONCODE,
	ITEMLEDGERENTRYNO,
	VALUEDQUANTITY,
	ITEMLEDGERENTRYQUANTITY,
	INVOICEDQUANTITY,
	COSTAMOUNTACTUAL,
	COSTPOSTEDTOG_L,
	COSTAMOUNTACTUALACY,
	COSTPOSTEDTOG_LACY,
	EXPECTEDCOST,
	COSTAMOUNTEXPECTED,
	COSTAMOUNTEXPECTEDACY,
	EXPECTEDCOSTPOSTEDTOG_L,
	EXP_COSTPOSTEDTOG_LACY,
	VARIANTCODE
) as 
			  SELECT CompanyName,
                     
                     itemNo  ,
                     locationCode  ,
                     itemLedgerEntryNo  ,
					  
                     SUM(valuedQuantity) valuedQuantity ,
                      SUM(itemLedgerEntryQuantity) ItemLedgerEntryQuantity,
                      SUM(invoicedQuantity) InvoicedQuantity,
                     SUM( costAmountActual) CostAmountActual,
                     SUM(costPostedToGL) CostPostedtoG_L,
                      
                     SUM(costAmountActualACY) CostAmountActualACY,
                     SUM(costPostedToGLACY) CostPostedtoG_LACY,
                      
                     SUM(costAmountExpected) ExpectedCost, 
                      
                     SUM(costAmountExpected) CostAmountExpected,
                     
                     SUM(costAmountExpectedACY) CostAmountExpectedACY,
                     
                     SUM(expectedCostPostedToGL) ExpectedCostPostedtoG_L,
                     SUM(expCostPostedToGLACY) Exp_CostPostedtoG_LACY, 
                     variantCode  from fco.silver.factValueEntry
					 GROUP BY CompanyName, 
                              itemNo,
                              locationCode,
                              itemLedgerEntryNo, 
                              variantCode;


create or replace view FCO.GOLD.VFACTWEEKLYITEMPRICING(
	COMPANYNAME,
	DATEDAY,
	ITEMNO,
	ITEMSKEY,
	VARIANTCODE,
	ITEMVARIANTSKEY,
	ITEMPRICE,
	VARIANTPRICE
) as
WITH Dates AS (
 SELECT *
FROM fco.silver.DimDate
WHERE MonthID >= 202306
  AND try_to_date(DateDay) <= DATEADD(MM,1,GETDATE())
  AND DayOfWeekID = 6
),
Item AS (
    SELECT *
    FROM fco.gold.vDimItem
),
ItemVariant AS (
    SELECT *
    FROM fco.gold.vDimItemVariant
),
--Locations AS (
--    SELECT * 
--    FROM [SaaS].[vDimLocation]
--),
tempMDM AS (
    SELECT
        SP.CompanyName,
        SP.salesCode  ,
        SP.itemNo  ,
        SP.variantCode ,
       try_to_date(startingDate) startingDate,
        try_to_date(endingDate) endingDate,
        SP.unitPrice  ,
        ROW_NUMBER() OVER (
            PARTITION BY SP.CompanyName, SP.itemNo, SP.variantCode, try_to_date(startingDate)
            ORDER BY SP.CompanyName, SP.itemNo, SP.variantCode, try_to_date(startingDate) DESC
        ) AS RowNumber
    FROM fco.silver.factSalesPrice SP 
 
    WHERE SP.salesCode = 'ALL'  
      AND SP.variantCode != ''
),
tempMDM2 AS (
    SELECT
        SP.CompanyName,
        SP.salesCode ,
        SP.itemNo  , 
        SP.variantCode  ,
        try_to_date(startingDate) startingDate,
        try_to_date(endingDate) endingDate,
        SP.unitPrice  ,
        ROW_NUMBER() OVER (
            PARTITION BY SP.CompanyName, SP.itemNo, SP.variantCode, try_to_date(startingDate)
            ORDER BY SP.CompanyName, SP.itemNo, SP.variantCode, try_to_date(startingDate) DESC
        ) AS RowNumber
    FROM fco.silver.factSalesPrice SP
 
    WHERE SP.salesCode = 'ALL'  
      AND SP.variantCode = ''
)

SELECT 
    b.CompanyName,
	try_to_date(a.DateDay) DateDay,
    --c.[LocationCode] AS [Location Code], LocationSKey,
    b.itemNo, ItemSKey,
    b.Variant VariantCode, ItemVariantSKey,
    COALESCE(SP2.UnitPrice, d.Unit_Price) AS ItemPrice,
    COALESCE(SP.UnitPrice,SP2.UnitPrice, d.Unit_Price) AS VariantPrice
FROM Dates a
CROSS JOIN ItemVariant b
--LEFT JOIN Locations c
--    ON b.CompanyName = c.CompanyName
LEFT JOIN tempMDM SP
    ON b.CompanyName   = SP.CompanyName
    AND b.itemNo = SP.itemNo
    AND b.Variant = SP.VariantCode
    AND (a.DateDay >= SP.startingDate OR SP.startingDate = '0001-01-01')
    AND (a.DateDay <= SP.EndingDate OR SP.EndingDate = '0001-01-01')
    AND SP.RowNumber = 1
LEFT JOIN tempMDM2 SP2
    ON b.CompanyName   = SP2.CompanyName
    AND b.itemNo = SP2.itemNo
    AND (a.DateDay >= SP2.startingDate OR SP2.startingDate = '1753-01-01 00:00:00.000')
    AND (a.DateDay <= SP2.EndingDate OR SP2.EndingDate = '1753-01-01 00:00:00.000')
    AND SP2.RowNumber = 1
LEFT JOIN Item d
ON b.CompanyName = d.CompanyName
AND b.itemNo = d.No_;


create or replace view FCO.GOLD.VFACTWEEKLYITEMPRICING(
	COMPANYNAME,
	DATEDAY,
	ITEMNO,
	ITEMSKEY,
	VARIANTCODE,
	ITEMVARIANTSKEY,
	ITEMPRICE,
	VARIANTPRICE
) as
WITH Dates AS (
 SELECT *
FROM fco.silver.DimDate
WHERE MonthID >= 202306
  AND try_to_date(DateDay) <= DATEADD(MM,1,GETDATE())
  AND DayOfWeekID = 6
),
Item AS (
    SELECT *
    FROM fco.gold.vDimItem
),
ItemVariant AS (
    SELECT *
    FROM fco.gold.vDimItemVariant
),
--Locations AS (
--    SELECT * 
--    FROM [SaaS].[vDimLocation]
--),
tempMDM AS (
    SELECT
        SP.CompanyName,
        SP.salesCode  ,
        SP.itemNo  ,
        SP.variantCode ,
       try_to_date(startingDate) startingDate,
        try_to_date(endingDate) endingDate,
        SP.unitPrice  ,
        ROW_NUMBER() OVER (
            PARTITION BY SP.CompanyName, SP.itemNo, SP.variantCode, try_to_date(startingDate)
            ORDER BY SP.CompanyName, SP.itemNo, SP.variantCode, try_to_date(startingDate) DESC
        ) AS RowNumber
    FROM fco.silver.factSalesPrice SP 
 
    WHERE SP.salesCode = 'ALL'  
      AND SP.variantCode != ''
),
tempMDM2 AS (
    SELECT
        SP.CompanyName,
        SP.salesCode ,
        SP.itemNo  , 
        SP.variantCode  ,
        try_to_date(startingDate) startingDate,
        try_to_date(endingDate) endingDate,
        SP.unitPrice  ,
        ROW_NUMBER() OVER (
            PARTITION BY SP.CompanyName, SP.itemNo, SP.variantCode, try_to_date(startingDate)
            ORDER BY SP.CompanyName, SP.itemNo, SP.variantCode, try_to_date(startingDate) DESC
        ) AS RowNumber
    FROM fco.silver.factSalesPrice SP
 
    WHERE SP.salesCode = 'ALL'  
      AND SP.variantCode = ''
)

SELECT 
    b.CompanyName,
	try_to_date(a.DateDay) DateDay,
    --c.[LocationCode] AS [Location Code], LocationSKey,
    b.itemNo, ItemSKey,
    b.Variant VariantCode, ItemVariantSKey,
    COALESCE(SP2.UnitPrice, d.Unit_Price) AS ItemPrice,
    COALESCE(SP.UnitPrice,SP2.UnitPrice, d.Unit_Price) AS VariantPrice
FROM Dates a
CROSS JOIN ItemVariant b
--LEFT JOIN Locations c
--    ON b.CompanyName = c.CompanyName
LEFT JOIN tempMDM SP
    ON b.CompanyName   = SP.CompanyName
    AND b.itemNo = SP.itemNo
    AND b.Variant = SP.VariantCode
    AND (a.DateDay >= SP.startingDate OR SP.startingDate = '0001-01-01')
    AND (a.DateDay <= SP.EndingDate OR SP.EndingDate = '0001-01-01')
    AND SP.RowNumber = 1
LEFT JOIN tempMDM2 SP2
    ON b.CompanyName   = SP2.CompanyName
    AND b.itemNo = SP2.itemNo
    AND (a.DateDay >= SP2.startingDate OR SP2.startingDate = '1753-01-01 00:00:00.000')
    AND (a.DateDay <= SP2.EndingDate OR SP2.EndingDate = '1753-01-01 00:00:00.000')
    AND SP2.RowNumber = 1
LEFT JOIN Item d
ON b.CompanyName = d.CompanyName
AND b.itemNo = d.No_;



