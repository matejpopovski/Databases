SELECT COUNT(*)
      FROM(SELECT COUNT(*) AS NumOfCategories
            FROM Category C, Item I
            WHERE C.ItemId = I.ItemId 
            GROUP BY C.ItemId
            HAVING NumOfCategories = 4
            )