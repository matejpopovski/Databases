SELECT COUNT(DISTINCT B.UserId) AS NumOf_SellAndBid
        FROM Bid B, Item I
        WHERE B.UserId  = I.UserId