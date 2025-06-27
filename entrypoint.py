import os

import pandas as pd
import sqlalchemy as sa

# Секреты MySQL


def get_mysql_url() -> str:
    url = os.environ["mysql_url"]
    return url


def get_postgres_url() -> str:
    url = os.environ["postgres_url"]
    return url


def main():
    # Выгрузка за сегодня из MySQL
    select = """
                WITH three_left_cols AS (
            	    SELECT 
            			DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d') AS  'start_time',
            			COUNT(t_bike_use.ride_amount) AS 'poezdok',
            			SUM(IFNULL(t_bike_use.ride_amount,0)) AS 'obzchaya_stoimost',
            			SUM(IFNULL(t_bike_use.discount,0)) AS 'oplacheno_bonusami',
            			SUM(IFNULL(t_bike_use.duration,0)) / 60 AS 'obschee_vremya_min'
            			FROM shamri.t_bike_use
            	WHERE t_bike_use.ride_status!=5
            	GROUP BY DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d')
            ),
            sum_uspeh_abon AS(
            	SELECT 
            		DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') AS start_time,
            		sum(IFNULL(t_trade.amount,0)) AS vyruchka_s_abonementov
            	FROM t_trade
            	WHERE t_trade.`type` = 6 AND t_trade.status = 1
            	GROUP BY start_time
            	),
            sum_mnogor_abon AS (
            	SELECT 
            		DATE_FORMAT(t_subscription_mapping.start_time, '%Y-%m-%d') AS start_time,
            		sum(IFNULL(t_subscription.price,0)) AS sum_mnogor_abon
            	FROM t_subscription_mapping
            	LEFT JOIN t_subscription ON t_subscription_mapping.subscription_id = t_subscription.id
            	GROUP BY DATE_FORMAT(t_subscription_mapping.start_time, '%Y-%m-%d')
            ),
            kvt AS (
            	SELECT 
            		DATE_FORMAT(FROM_UNIXTIME(t_bike.heart_time), '%Y-%m-%d') AS start_time,
            		COUNT(t_bike.id) AS kvt
            	FROM t_bike
            	WHERE TIMESTAMPDIFF(SECOND, now(), DATE_FORMAT(FROM_UNIXTIME(t_bike.g_time), '%Y-%m-%d %H:%m:%s')) < 900 AND t_bike.error_status IN (0, 7)
            	GROUP BY DATE_FORMAT(FROM_UNIXTIME(t_bike.heart_time), '%Y-%m-%d')
            	LIMIT 1
            ),
            vyruchka_payTabs_stripe AS ( 
            	SELECT
            		vyruchka_v_statuse_1.start_time,
            		vyruchka_v_statuse_1.vyruchka_v_statuse_1,
            		IFNULL(vozvraty.vozvraty,0) AS 'vozvraty', 
            		vyruchka_v_statuse_1.vyruchka_v_statuse_1 - IFNULL(vozvraty.vozvraty,0) AS 'vyruchka_payTabs',
            		IFNULL(stripe_1.stripe_1, 0) AS 'stripe_1',
            		IFNULL(stripe_4.stripe_4, 0) AS 'stripe_4',
            		IFNULL(stripe_1.stripe_1, 0) - IFNULL(stripe_4.stripe_4, 0) AS 'vyruchka_stripe'
            FROM 
            	(SELECT 
            		DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') AS start_time,
            		SUM(IFNULL(t_trade.account_pay_amount,0)) AS 'vyruchka_v_statuse_1'
            	FROM t_trade
            	WHERE t_trade.status=1 AND t_trade.way=26 AND t_trade.`type` IN (1,2,6,7)
            	GROUP BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d')
            	) AS vyruchka_v_statuse_1
            LEFT JOIN 	
            	(SELECT 
            		DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') AS start_time,
            		SUM(IFNULL(t_trade.account_pay_amount,0)) AS 'vozvraty'
            	 FROM t_trade
            	 WHERE t_trade.status=4 AND t_trade.way=26
            	 GROUP BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d')
            ) AS vozvraty ON vyruchka_v_statuse_1.start_time=vozvraty.start_time
            LEFT JOIN 
            	(SELECT 
            			DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') AS start_time,
            			SUM(IFNULL(t_trade.account_pay_amount,0)) AS 'stripe_1'
            		 FROM t_trade
            		 WHERE t_trade.status=1 AND t_trade.way=6
            		 GROUP BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d')
            	) AS stripe_1 
            ON vyruchka_v_statuse_1.start_time=stripe_1.start_time
            LEFT JOIN 
            	(SELECT 
            			DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') AS start_time,
            			SUM(IFNULL(t_trade.account_pay_amount,0)) AS 'stripe_4'
            		 FROM t_trade
            		 WHERE t_trade.status=4 AND t_trade.way=6
            		 GROUP BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d')
            	) AS stripe_4	
            ON vyruchka_v_statuse_1.start_time=stripe_4.start_time
            ),
            user_v_den_register AS (
            SELECT 
            	DATE_FORMAT(t_user.register_date, '%Y-%m-%d') AS start_time,
            	COUNT(t_user.id) AS 'user_v_den_register'
            FROM t_user
            GROUP BY DATE_FORMAT(t_user.register_date, '%Y-%m-%d')
            ),
            kolichestvo_novyh_s_1_poezdkoy AS ( 
            	SELECT
            		register_date_as_start_date.start_date,
            		COUNT(DISTINCT register_date_as_start_date.uid) AS 'kolichestvo_novyh_s_1_poezdkoy',
            		COUNT(register_date_as_start_date.ride_amount) AS 'kolichestvo_poezdok_vsego'
            	FROM
            		(SELECT 
            			DATE(DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d')) AS start_date, 
            			t_bike_use.*,
            			register_users.register_date,
            			register_users.id AS register_user_id
            		FROM t_bike_use
            		LEFT JOIN (
            			SELECT 
            				DATE(DATE_FORMAT(t_user.register_date, '%Y-%m-%d')) AS register_date, 
            				t_user.id
            			FROM t_user
            							) AS register_users
            		ON t_bike_use.uid=register_users.id
            		WHERE DATE(DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d')) = DATE(DATE_FORMAT(register_users.register_date, '%Y-%m-%d')) 
            		AND t_bike_use.ride_status!=5
            		) AS register_date_as_start_date
            	GROUP BY register_date_as_start_date.start_date
            ),
            dolgi AS (
            	SELECT 
            		DATE_FORMAT(t_payment_details.created, '%Y-%m-%d') AS 'create_debit_date', 
            		SUM(IFNULL(t_payment_details.debit_cash,0)) AS 'dolgi'
            	FROM t_payment_details
            	RIGHT JOIN (
            		SELECT 
            			t_bike_use.*
            		FROM t_bike_use
            		WHERE t_bike_use.ride_status = 2
            		ORDER BY t_bike_use.id DESC
            	) AS dolgovye_poezdki
            	ON t_payment_details.user_id = dolgovye_poezdki.uid AND t_payment_details.ride_id = dolgovye_poezdki.id
            	GROUP BY DATE_FORMAT(t_payment_details.created, '%Y-%m-%d')
            )
            SELECT 
            	NOW() AS 'timestamp',
            	-- CAST(DATE_FORMAT(three_left_cols.start_time, '%Y-%m-%d %h:%m:%s') AS datetime) AS 'day_', 
            	IFNULL(three_left_cols.poezdok,0)  AS 'poezdok',
            	IFNULL(three_left_cols.poezdok / kvt.kvt,0) AS 'poezdok_v_srednem_na_samokat',
            	IFNULL((IFNULL(three_left_cols.obzchaya_stoimost,0) - IFNULL(sum_mnogor_abon.sum_mnogor_abon,0) - IFNULL(three_left_cols.oplacheno_bonusami,0) + IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0)),0) / IFNULL(kvt.kvt,0) AS 'vyruchka_sim',
            	(IFNULL(three_left_cols.obzchaya_stoimost,0) - IFNULL(sum_mnogor_abon.sum_mnogor_abon,0) - IFNULL(three_left_cols.oplacheno_bonusami,0) + IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0)) / IFNULL(three_left_cols.poezdok,0) AS 'srednyaa_cena_poezdki',
            	IFNULL(three_left_cols.oplacheno_bonusami,0) AS 'oplacheno_bonusami',
            	IFNULL(three_left_cols.obzchaya_stoimost,0) - IFNULL(dolgi.dolgi,0) - (IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0) - IFNULL(sum_mnogor_abon.sum_mnogor_abon,0)) AS 'vyruchka',
            	IFNULL(three_left_cols.obzchaya_stoimost,0) - IFNULL(dolgi.dolgi,0) - (IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0) - IFNULL(sum_mnogor_abon.sum_mnogor_abon,0)) - IFNULL(three_left_cols.oplacheno_bonusami,0) AS 'vyruchka_bez_bonusov',
            	IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0) AS 'vyruchka_s_abonementov',
            	IFNULL(three_left_cols.obzchaya_stoimost,0) - IFNULL(dolgi.dolgi,0) - (IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0) - IFNULL(sum_mnogor_abon.sum_mnogor_abon,0)) - IFNULL(three_left_cols.oplacheno_bonusami,0) + IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0) AS 'vyruchka_bez_bonusov_vyruchka_s_abonementov',
            	-- IFNULL(three_left_cols.obzchaya_stoimost,0) AS 'obschaya_stoimost',
            	-- IFNULL(dolgi.dolgi,0) AS 'dolgi', 
            	-- IFNULL(sum_mnogor_abon.sum_mnogor_abon,0) AS 'vyruchka_s_mnogor_abonementov',
            	SUM(IFNULL(three_left_cols.obzchaya_stoimost, 0) - IFNULL(dolgi.dolgi,0) - (IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0) - IFNULL(sum_mnogor_abon.sum_mnogor_abon,0)) + IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0)) OVER (ORDER BY DATE(three_left_cols.start_time)) AS 'vni',
            	SUM(IFNULL(three_left_cols.obzchaya_stoimost, 0) - IFNULL(dolgi.dolgi,0) - (IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0) - IFNULL(sum_mnogor_abon.sum_mnogor_abon,0)) + IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0)) OVER (ORDER BY DATE(three_left_cols.start_time)) - SUM(IFNULL(three_left_cols.oplacheno_bonusami,0)) OVER (ORDER BY DATE(three_left_cols.start_time)) AS 'vni_bez_bonusov',
            	IFNULL(vyruchka_payTabs_stripe.vyruchka_payTabs,0) AS 'vyruchka PayTabs',
            	SUM(IFNULL(vyruchka_payTabs_stripe.vyruchka_payTabs,0)) OVER (ORDER BY DATE(three_left_cols.start_time)) AS 'obsch_PayTabs',
            	IFNULL(vyruchka_payTabs_stripe.vyruchka_stripe,0) AS 'vyruchka_Stripe',
            	SUM(IFNULL(vyruchka_payTabs_stripe.vyruchka_stripe,0)) OVER (ORDER BY DATE(three_left_cols.start_time)) AS 'obsch_Stripe',
            	SUM(IFNULL(vyruchka_payTabs_stripe.vyruchka_payTabs,0)) OVER (ORDER BY DATE(three_left_cols.start_time)) + SUM(IFNULL(vyruchka_payTabs_stripe.vyruchka_stripe,0)) OVER (ORDER BY DATE(three_left_cols.start_time)) AS 'obsch_PayTabs_i_obsch_Stripe',
            	IFNULL(kvt.kvt,0) AS 'kvt',
            	IFNULL(user_v_den_register.user_v_den_register,0) AS 'user_v_den',
            	SUM(IFNULL(user_v_den_register.user_v_den_register,0)) OVER (ORDER BY DATE(three_left_cols.start_time)) AS 'user_v_den_obshc',
            	IFNULL(three_left_cols.obschee_vremya_min,0) AS 'obcshee_vrmeya_min',
            	IFNULL(three_left_cols.obschee_vremya_min,0) / IFNULL(three_left_cols.poezdok,0) AS 'srednyee_vremya_poezdki',
            	IFNULL(kolichestvo_novyh_s_1_poezdkoy.kolichestvo_novyh_s_1_poezdkoy,0) AS 'iz_nih_usero_sovershivshih_poezdki_vsego',
            	IFNULL(kolichestvo_novyh_s_1_poezdkoy.kolichestvo_poezdok_vsego,0) AS 'kol_vo_poezdok_vsego',
            	ROUND((IFNULL(kolichestvo_novyh_s_1_poezdkoy.kolichestvo_novyh_s_1_poezdkoy,0) / IFNULL(user_v_den_register.user_v_den_register,0)) * 100, 2) AS 'proniknovenie',
            	ROUND(IFNULL(kolichestvo_novyh_s_1_poezdkoy.kolichestvo_poezdok_vsego,0) / IFNULL(kolichestvo_novyh_s_1_poezdkoy.kolichestvo_novyh_s_1_poezdkoy,0), 2) AS 'poezdok_novyi_user'
            FROM three_left_cols
            LEFT JOIN sum_uspeh_abon ON three_left_cols.start_time = sum_uspeh_abon.start_time
            LEFT JOIN sum_mnogor_abon ON three_left_cols.start_time = sum_mnogor_abon.start_time
            LEFT JOIN kvt ON three_left_cols.start_time = kvt.start_time
            LEFT JOIN vyruchka_payTabs_stripe ON three_left_cols.start_time = vyruchka_payTabs_stripe.start_time
            LEFT JOIN user_v_den_register ON three_left_cols.start_time = user_v_den_register.start_time
            LEFT JOIN kolichestvo_novyh_s_1_poezdkoy ON three_left_cols.start_time = kolichestvo_novyh_s_1_poezdkoy.start_date
            LEFT JOIN dolgi ON three_left_cols.start_time = dolgi.create_debit_date
            WHERE DATE_FORMAT(NOW(), '%Y-%m-%d') = three_left_cols.start_time
            ORDER BY three_left_cols.start_time DESC
    """
    select1 = '''
    SELECT NOW() AS  'timestamp' 
    '''

    url = get_mysql_url()
    url = sa.engine.make_url(url)
    url = url.set(drivername="mysql+mysqlconnector")
    engine_mysql = sa.create_engine(url)
    df_vni = pd.read_sql(select, engine_mysql)

    # Загрузка за сегодня в Postgres
    url = get_postgres_url()
    url = sa.engine.make_url(url)
    url = url.set(drivername="postgresql+psycopg")
    engine_postgresql = sa.create_engine(url)
    df_vni.to_sql("vni_total", engine_postgresql, if_exists="append", index=False)
    
    print('Got it!')


if __name__ == "__main__":
    main()
