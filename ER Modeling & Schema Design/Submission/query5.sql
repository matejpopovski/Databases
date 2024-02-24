SELECT COUNT(DISTINCT U.UserId) AS NumSellersWithHighRating
FROM User U
JOIN Item I ON U.UserId = I.UserId
WHERE U.Rating > 1000