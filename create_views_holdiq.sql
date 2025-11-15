-- create_views_holdiq.sql
CREATE VIEW IF NOT EXISTS v_big_qoq_buys AS
SELECT manager_cik, report_period, issuer, cusip, value_delta, pct_of_port, rank_in_port
FROM positions_13f_delta
WHERE action='INCREASE'
ORDER BY value_delta DESC;

CREATE VIEW IF NOT EXISTS v_new_positions AS
SELECT manager_cik, report_period, issuer, cusip, value_now AS initiated_value, pct_of_port, rank_in_port
FROM positions_13f_delta
WHERE action='NEW'
ORDER BY initiated_value DESC;

CREATE VIEW IF NOT EXISTS v_sold_out AS
SELECT manager_cik, report_period, issuer, cusip, -value_delta AS exited_value
FROM positions_13f_delta
WHERE action='SOLD_OUT'
ORDER BY exited_value DESC;

CREATE VIEW IF NOT EXISTS v_insider_net_30d AS
SELECT issuer_cik, issuer_ticker, issuer_name,
       SUM(CASE WHEN side='BUY' THEN shares ELSE 0 END) AS sh_buy,
       SUM(CASE WHEN side='SELL' THEN shares ELSE 0 END) AS sh_sell,
       SUM(CASE WHEN side='BUY' THEN shares*price ELSE 0 END) -
       SUM(CASE WHEN side='SELL' THEN shares*price ELSE 0 END) AS net_usd
FROM insider_tx
WHERE trans_date >= DATE('now','-30 day')
GROUP BY 1,2,3
ORDER BY net_usd DESC;
