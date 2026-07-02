# =====================================================================
# 03 · DATA QUALITY — Pipeline gate
# Tiered checks: HARD checks fail the job; WARN checks only log.
# Prevents bad data from reaching the Gold layer.
# =====================================================================

hard_checks = {
    "silver.orders null order_id":
        "SELECT COUNT(*) FROM workspace.silver.orders WHERE order_id IS NULL",
    "silver.orders negative amount":
        "SELECT COUNT(*) FROM workspace.silver.orders WHERE total_amount < 0",
    "silver.orders duplicate keys":
        "SELECT COUNT(*) FROM (SELECT order_id FROM workspace.silver.orders "
        "GROUP BY order_id HAVING COUNT(*) > 1)",
}

warn_checks = {
    "orphan orders (no customer)":
        "SELECT COUNT(*) FROM workspace.silver.orders o "
        "LEFT JOIN workspace.silver.customers c ON o.customer_id=c.customer_id "
        "WHERE c.customer_id IS NULL",
}

failures = []
for name, sql in hard_checks.items():
    n = spark.sql(sql).collect()[0][0]
    print(("OK   " if n == 0 else "FAIL ") + f"{name}: {n}")
    if n > 0:
        failures.append(f"{name} = {n}")

for name, sql in warn_checks.items():
    n = spark.sql(sql).collect()[0][0]
    print(("OK   " if n == 0 else "WARN ") + f"{name}: {n}")

if failures:
    raise Exception("DATA QUALITY FAILED: " + "; ".join(failures))
print("All hard data quality checks passed.")
