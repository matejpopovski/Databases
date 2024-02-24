SELECT COUNT(DISTINCT c.Category_Name) AS Number_of_Categories
FROM Category c,  Bid b
WHERE c.ItemId = b.ItemId
AND b.Amount > 100 
AND b.Amount != "NULL"