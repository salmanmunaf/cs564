SELECT count() 
FROM Item
WHERE Item.ItemID in (SELECT Category.ItemID
                FROM Category 
                GROUP BY Category.ItemID
                HAVING count() = 4);