# order_management
This project contains 3 different ksqlDB UDFs that perform the following tasks 
1) convert a julian date stored as a string into epoch time (Long). for Example '121030' converts to 1611964800000, which represents January 30, 2021. 
Note that 121030 + 1900000 equals 2021030 which represents Jan 30 2021 in the 'yyyyDDD' format.
2) Check the status of a salesforce order based on defined business rules.
3) Update the confirmed delivery date of an order based on business rules
