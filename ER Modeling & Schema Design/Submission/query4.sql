SELECT I.ItemId
        FROM Item I
        JOIN (
            SELECT MAX(Currently) AS MAX_PRICE
            FROM Item
        ) AS MaxPrice
        ON I.Currently = MaxPrice.MAX_PRICE