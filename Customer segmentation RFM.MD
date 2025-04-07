use Retail;
   /* Segmenting customers using the Recency, Frequency and Monetary parameters to rank customers in percentiles to understand  
   differenct customer segments for marketing and sales promotions*/

	  select * into Temporary_table from
	  
				(select Customer_ID, Round((PERCENT_RANK() over (order by recency)*10),0) as R,  Round((PERCENT_RANK() over (order by frequency desc)*10),0) as F, 
				round((PERCENT_RANK() over (order by monetary desc)*10),0) AS M FROM (select Customer_ID, COUNT(Transaction_ID) as 
				frequency, datediff(day, MAX(Date), cast(GETDATE() as date)) as recency,
				Round(SUM(Amount),1) as monetary from dbo.retail group by Customer_ID) sub) su
	-----------------------------------------VIP or Top-Tier Customers-------------------------------------------------------------
	   select Customer_ID, R,F,M from Temporary_table where R>8 AND F>8 and M>8 /*Perfect customers to upsell/cross-sell to*/

	   ---------------------------------------Loyalists or middle-class Customers-------------------------------------
	   select Customer_ID, R,F,M from Temporary_table where (R>=5 and R<=7) AND (F>=5 and F<=7) AND (M>=5 and M<=7) /*Perfect customers to promote loyalty programs*/

	   ----------------------------------------Bottom-tier customers-------------------------------------------------------------------

	   select Customer_ID, R,F,M from Temporary_table where R<5 AND F<5 and M<5 order by customer_id /*Perfect customers to nuture with more educational content and get feedback from */

