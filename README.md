# nba-data-lakehouse
End-to-end NBA Data Engineering project using Medallion Architecture, Python, and DuckDB.

Challenge: Implemented advanced bot-bypass logic including Hive-style partitioning, CDN backdoor access, and jittered request pacing to overcome Akamai rate-limiting on the NBA's stats API.

I used a Lambda Architecture approach. I use the Official Boxscore as my control layer for accuracy (Top-Down), and I process the Play-by-Play (Bottom-Up) to engineer advanced features like spatial tracking and clutch metrics, creating a unified Silver Layer View
