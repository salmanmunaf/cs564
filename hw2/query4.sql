SELECT Item.ItemID
FROM Item
WHERE Item.Currently = (SELECT max(Item.Currently) FROM Item);