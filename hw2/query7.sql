SELECT COUNT(DISTINCT C.Category)
FROM Category C
LEFT JOIN Item I
ON I.ItemID = C.ItemID
WHERE I.Currently > 100
AND Number_of_Bids > 0;