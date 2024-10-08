# Databricks notebook source
print("Hello bobo!")

# COMMAND ----------

lunch_price = 20

if lunch_price < 20:
    print("Pasok sa budget mo boi!")
else:
    print("Pulubi!")

# COMMAND ----------

if lunch_price < 20:
    print("Pasok sa budget mo boi!")
else:
    if lunch_price < 30:
        print("Pasok pa din sa budget mo boi!")
    else:
        print("Pulubi!")

# COMMAND ----------

lunch_price = 15

if lunch_price == 10:
    print("Puta 10 lang? Ano yan mura na, madumi pa?")
elif lunch_price <= 15:
    print("Pasok sa budget mo boi!")
elif lunch_price > 20:
    print("Ano yan gold?")
else: 
    print("Tanginang yan!")

# COMMAND ----------


