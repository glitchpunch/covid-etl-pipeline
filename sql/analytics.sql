-- Top 10 countries by peak 7-day average of new cases
SELECT country, MAX(new_confirmed_7d_avg)::int AS peak_7d_avg
FROM daily_stats GROUP BY country ORDER BY peak_7d_avg DESC LIMIT 10;

-- Month-over-month change in new cases for one country
SELECT year, month, new_confirmed,
       new_confirmed - LAG(new_confirmed) OVER (ORDER BY year, month) AS mom_change
FROM monthly_summary WHERE country = 'India' ORDER BY year, month;
