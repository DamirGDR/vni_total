import os

import pandas as pd
import sqlalchemy as sa
import json
import google.oauth2.service_account
import googleapiclient.discovery

# Секреты MySQL


def get_mysql_url() -> str:
    url = os.environ["mysql_url"]
    return url


def get_postgres_url() -> str:
    url = os.environ["postgres_url"]
    return url


def get_google_creds() -> str:
    url = os.environ["google_service_account_json"]
    return url


def read_sheet_data_to_pandas(service, spreadsheet_id: str, range_name: str):
    """
    Читает данные из Google Таблицы по указанному диапазону и преобразует их в Pandas DataFrame.

    Args:
        service: Объект службы Google Sheets API.
        spreadsheet_id: ID Google Таблицы.
        range_name: Диапазон ячеек (например, 'Sheet1!A1:D10').

    Returns:
        Pandas DataFrame с данными или None в случае ошибки.
    """
    if not service:
        return None

    try:
        # Выполняем запрос к Sheets API для получения значений
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            majorDimension='ROWS' # Получаем данные по строкам
        ).execute()

        values = result.get('values', [])

        if not values:
            print(f"В диапазоне '{range_name}' таблицы '{spreadsheet_id}' нет данных.")
            return pd.DataFrame() # Возвращаем пустой DataFrame

        # Pandas может напрямую создать DataFrame из списка списков.
        # Обычно первая строка содержит заголовки.
        headers = values[0]
        data_rows = values[1:]

        # Создаем Pandas DataFrame
        if headers:
            # Преобразуем данные в DataFrame, используя первую строку как заголовки
            df = pd.DataFrame(data_rows, columns=headers)
        else:
            # Если заголовков нет, просто создаем DataFrame из данных
            df = pd.DataFrame(values)
            print("Предупреждение: Заголовки не обнаружены. Столбцы названы автоматически (0, 1, 2...).")

        print(f"Данные из диапазона '{range_name}' успешно прочитаны и преобразованы в Pandas DataFrame.")
        return df

    except googleapiclient.errors.HttpError as error:
        print(f"Ошибка Google Sheets API: {error}")
        print(f"Код ошибки: {error.resp.status}")
        print(f"Сообщение об ошибке: {error._get_reason()}")
        return None
    except Exception as e:
        print(f"Произошла непредвиденная ошибка: {e}")
        return None


def get_sheets_service(service_account_file: str):
    """
    Создает и возвращает объект службы Google Sheets API.

    Args:
        service_account_file: Путь к JSON-файлу учетных данных сервисного аккаунта.

    Returns:
        Объект googleapiclient.discovery.Resource для Sheets API.
    """
    try:
        # Определяем области доступа. Для чтения достаточно 'spreadsheets.readonly'.
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

        # Загружаем учетные данные сервисного аккаунта
        creds = google.oauth2.service_account.Credentials.from_service_account_file(
            service_account_file, scopes=SCOPES
        )

        # Строим сервис Sheets API
        service = googleapiclient.discovery.build('sheets', 'v4', credentials=creds)
        print("Сервис Google Sheets API успешно инициализирован.")
        return service
    except Exception as e:
        print(f"Ошибка при инициализации сервиса Google Sheets API: {e}")
        return None


def main():
    # Выгрузка за сегодня из MySQL
    select_vni_total = """
	                -- Из Datalens
	-- ВНИ Общий
	WITH three_left_cols AS (
		    SELECT 
				DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d') AS  'start_time',
				COUNT(t_bike_use.ride_amount) AS 'poezdok',
				SUM(IFNULL(t_bike_use.ride_amount,0)) AS 'obzchaya_stoimost',
				SUM(IFNULL(t_bike_use.discount,0)) AS 'oplacheno_bonusami',
				SUM(IFNULL(t_bike_use.duration,0)) / 60 AS 'obschee_vremya_min'
				FROM shamri.t_bike_use
		WHERE t_bike_use.ride_status!=5 AND DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d') >= '2024-07-21'
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
		chastichno_vozvrascheny.chastichno_vozvrascheny AS 'chastichno_vozvrascheny',
		IFNULL(vyruchka_v_statuse_1.vyruchka_v_statuse_1,0) + IFNULL(chastichno_vozvrascheny.chastichno_vozvrascheny,0) AS 'vyruchka_payTabs',
		IFNULL(stripe_1.stripe_1, 0) AS 'stripe_1',
		IFNULL(stripe_4.stripe_4, 0) AS 'stripe_4',
		IFNULL(stripe_1.stripe_1, 0) - IFNULL(stripe_4.stripe_4, 0) AS 'vyruchka_stripe'
	FROM 
		(
		SELECT 
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
		) AS stripe_1 ON vyruchka_v_statuse_1.start_time=stripe_1.start_time
	LEFT JOIN 
		(SELECT 
				DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') AS start_time,
			SUM(IFNULL(t_trade.account_pay_amount,0)) AS 'stripe_4'
		 FROM t_trade
		 WHERE t_trade.status=4 AND t_trade.way=6
		 GROUP BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d')
		) AS stripe_4 ON vyruchka_v_statuse_1.start_time=stripe_4.start_time
	LEFT JOIN 
		(
		SELECT 
			DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') AS start_time,
			SUM(IFNULL(t_trade.account_pay_amount,0)) AS 'chastichno_vozvrascheny'
		FROM t_trade
		WHERE t_trade.status=3
		GROUP BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d')
		) AS chastichno_vozvrascheny ON vyruchka_v_statuse_1.start_time = chastichno_vozvrascheny.start_time
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
		-- IFNULL(three_left_cols.obzchaya_stoimost,0) AS 'obschaya_stoimost',
		-- IFNULL(dolgi.dolgi,0) AS 'dolgi',
		IFNULL(three_left_cols.oplacheno_bonusami,0) AS 'oplacheno_bonusami',
		IFNULL(three_left_cols.obzchaya_stoimost,0) - IFNULL(dolgi.dolgi,0) - (IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0) - IFNULL(sum_mnogor_abon.sum_mnogor_abon,0)) AS 'vyruchka',
		IFNULL(three_left_cols.obzchaya_stoimost,0) - IFNULL(dolgi.dolgi,0) - (IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0) - IFNULL(sum_mnogor_abon.sum_mnogor_abon,0)) - IFNULL(three_left_cols.oplacheno_bonusami,0) AS 'vyruchka_bez_bonusov',
		IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0) AS 'vyruchka_s_abonementov', 
		-- IFNULL(sum_mnogor_abon.sum_mnogor_abon,0) AS 'vyruchka_s_mnogor_abonementov',
		IFNULL(three_left_cols.obzchaya_stoimost,0) - IFNULL(dolgi.dolgi,0) - (IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0) - IFNULL(sum_mnogor_abon.sum_mnogor_abon,0)) - IFNULL(three_left_cols.oplacheno_bonusami,0) + IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0) AS 'vyruchka_bez_bonusov_vyruchka_s_abonementov',
		SUM(IFNULL(three_left_cols.obzchaya_stoimost, 0) - IFNULL(dolgi.dolgi,0) - (IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0) - IFNULL(sum_mnogor_abon.sum_mnogor_abon,0)) + IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0)) OVER (ORDER BY DATE(three_left_cols.start_time)) AS 'vni',
		SUM(IFNULL(three_left_cols.obzchaya_stoimost, 0) - IFNULL(dolgi.dolgi,0) - (IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0) - IFNULL(sum_mnogor_abon.sum_mnogor_abon,0)) + IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0)) OVER (ORDER BY DATE(three_left_cols.start_time)) - SUM(IFNULL(three_left_cols.oplacheno_bonusami,0)) OVER (ORDER BY DATE(three_left_cols.start_time)) AS 'vni_bez_bonusov',
		--
		-- vyruchka_payTabs_stripe.vyruchka_v_statuse_1,
		-- vyruchka_payTabs_stripe.vozvraty,
		-- vyruchka_payTabs_stripe.chastichno_vozvrascheny,
		-- IFNULL(vyruchka_payTabs_stripe.vyruchka_v_statuse_1,0) - IFNULL(vyruchka_payTabs_stripe.vozvraty,0) AS 'vyruchka PayTabs_возвраты',
		-- SUM(IFNULL(vyruchka_payTabs_stripe.vyruchka_v_statuse_1,0) - IFNULL(vyruchka_payTabs_stripe.vozvraty,0)) OVER (ORDER BY DATE(three_left_cols.start_time)) AS 'obsch_PayTabs_возвраты',
		IFNULL(vyruchka_payTabs_stripe.vyruchka_v_statuse_1,0) + IFNULL(vyruchka_payTabs_stripe.chastichno_vozvrascheny,0) AS 'vyruchka PayTabs',
		SUM(IFNULL(vyruchka_payTabs_stripe.vyruchka_v_statuse_1,0) + IFNULL(vyruchka_payTabs_stripe.chastichno_vozvrascheny,0)) OVER (ORDER BY DATE(three_left_cols.start_time)) AS 'obsch_PayTabs',
		--
		IFNULL(vyruchka_payTabs_stripe.vyruchka_stripe,0) AS 'vyruchka_Stripe',
		SUM(IFNULL(vyruchka_payTabs_stripe.vyruchka_stripe,0)) OVER (ORDER BY DATE(three_left_cols.start_time)) AS 'obsch_Stripe',
		SUM(IFNULL(vyruchka_payTabs_stripe.vyruchka_payTabs,0)) OVER (ORDER BY DATE(three_left_cols.start_time)) + SUM(IFNULL(vyruchka_payTabs_stripe.vyruchka_stripe,0) + IFNULL(vyruchka_payTabs_stripe.chastichno_vozvrascheny,0)) OVER (ORDER BY DATE(three_left_cols.start_time)) AS 'obsch_PayTabs_i_obsch_Stripe',
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
	-- WHERE DATE_FORMAT(NOW(), '%Y-%m-%d') = three_left_cols.start_time
	ORDER BY three_left_cols.start_time DESC
	LIMIT 1
	    """
   
		

    url = get_mysql_url()
    url = sa.engine.make_url(url)
    url = url.set(drivername="mysql+mysqlconnector")
    engine_mysql = sa.create_engine(url)
    df_vni = pd.read_sql(select_vni_total, engine_mysql)

    # Загрузка за сегодня в Postgres
    url = get_postgres_url()
    url = sa.engine.make_url(url)
    url = url.set(drivername="postgresql+psycopg")
    engine_postgresql = sa.create_engine(url)
    df_vni.to_sql("vni_total", engine_postgresql, if_exists="append", index=False)
    
    print('Got it: VNI_total')

    select_vni_cities = '''
    -- ВНИ по городам, календарь. Без пропорции user в день
    -- Результат. Начинаю с списка городов t_city + календарь
    WITH three_left_cols AS 
    (    
        SELECT 
        three_left_cols.start_time,
        three_left_cols.city_id,
        three_left_cols.poezdok,
        three_left_cols.obzchaya_stoimost,
        SUM(IFNULL(three_left_cols.obzchaya_stoimost, 0)) OVER(PARTITION BY three_left_cols.city_id ORDER BY three_left_cols.start_time) AS 'obzchaya_stoimost_ni',
        SUM(IFNULL(three_left_cols.oplacheno_bonusami, 0)) OVER(PARTITION BY three_left_cols.city_id ORDER BY three_left_cols.start_time) AS 'oplacheno_bonusami_ni',
        three_left_cols.oplacheno_bonusami,
        three_left_cols.obschee_vremya_min
        FROM 
        (
            SELECT 
                DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d') AS  'start_time',
                t_bike.city_id,
                COUNT(t_bike_use.ride_amount) AS 'poezdok',
                SUM(IFNULL(t_bike_use.ride_amount,0)) AS 'obzchaya_stoimost',
                SUM(IFNULL(t_bike_use.discount,0)) AS 'oplacheno_bonusami',
                SUM(IFNULL(t_bike_use.duration,0)) / 60 AS 'obschee_vremya_min'
            FROM shamri.t_bike_use
            LEFT JOIN t_bike ON t_bike_use.bid = t_bike.id
            WHERE 
            t_bike_use.ride_status!=5 
            AND 
            DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d') >= '2024-07-21'
            GROUP BY DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d'), t_bike.city_id
        ) AS three_left_cols
        ORDER BY three_left_cols.start_time ASC
    ),
    sum_uspeh_abon AS (
        SELECT 
            distr_poezdki_po_gorodam.start_time,
            distr_poezdki_po_gorodam.city_id,
            distr_poezdki_po_gorodam.poezdok,
            distr_poezdki_po_gorodam.coef_goroda,
            sum_uspeh_abon.vyruchka_s_abonementov AS 'vyruchka_s_abonementov_po_vsem_gorodam',
            distr_poezdki_po_gorodam.coef_goroda AS 'coef_goroda_vyruchka_s_abonementov_po_vsem_gorodam',
            sum_uspeh_abon.vyruchka_s_abonementov * distr_poezdki_po_gorodam.coef_goroda AS 'vyruchka_s_abonementov',
            SUM(IFNULL(sum_uspeh_abon.vyruchka_s_abonementov, 0) * IFNULL(distr_poezdki_po_gorodam.coef_goroda,0)) OVER (PARTITION BY distr_poezdki_po_gorodam.city_id ORDER BY distr_poezdki_po_gorodam.start_time) AS 'vyruchka_s_abonementov_ni'
        FROM (
            -- высчитываю пропорции по поездкам
            SELECT 
                distr_poezdki_po_gorodam.start_time,
                distr_poezdki_po_gorodam.city_id,
                distr_poezdki_po_gorodam.poezdok,
                distr_poezdki_po_gorodam.poezdok / SUM(distr_poezdki_po_gorodam.poezdok) OVER (PARTITION BY distr_poezdki_po_gorodam.start_time) AS 'coef_goroda'
            FROM 
                (
                SELECT 
                    DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d') AS  'start_time',
                    t_bike.city_id,
                    COUNT(t_bike_use.ride_amount) AS 'poezdok'
                FROM shamri.t_bike_use
                LEFT JOIN t_bike ON t_bike_use.bid = t_bike.id
                WHERE t_bike_use.ride_status!=5 
                    AND DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d') >= '2024-07-21'
                GROUP BY DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d'), t_bike.city_id
                ORDER BY DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d') DESC
                ) 
                AS distr_poezdki_po_gorodam
            ORDER BY distr_poezdki_po_gorodam.start_time DESC
        ) AS distr_poezdki_po_gorodam
        LEFT JOIN (
            SELECT 
                DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') AS start_time,
                sum(IFNULL(t_trade.amount,0)) AS vyruchka_s_abonementov
            FROM shamri.t_trade
            WHERE t_trade.`type` = 6 
                AND t_trade.status = 1 
                 AND DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') >= '2024-07-21'
            GROUP BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d')
            ORDER BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') DESC
        ) AS sum_uspeh_abon
        ON distr_poezdki_po_gorodam.start_time = sum_uspeh_abon.start_time
        ORDER BY distr_poezdki_po_gorodam.start_time DESC
    ),
    sum_mnogor_abon AS (
        SELECT 
            distr_poezdki_po_gorodam.start_time,
            distr_poezdki_po_gorodam.city_id,
            distr_poezdki_po_gorodam.poezdok,
            distr_poezdki_po_gorodam.coef_goroda,
            sum_mnogor_abon.sum_mnogor_abon AS 'sum_mnogor_abon_po_vsem_gorodam',
            sum_mnogor_abon.sum_mnogor_abon * distr_poezdki_po_gorodam.coef_goroda AS 'sum_mnogor_abon',
            SUM(IFNULL(sum_mnogor_abon.sum_mnogor_abon,0) * IFNULL(distr_poezdki_po_gorodam.coef_goroda,0)) OVER (PARTITION BY distr_poezdki_po_gorodam.city_id ORDER BY distr_poezdki_po_gorodam.start_time) AS 'sum_mnogor_abon_ni'
        FROM (
    -- высчитываю пропорции по поездкам
        SELECT 
            distr_poezdki_po_gorodam.start_time,
            distr_poezdki_po_gorodam.city_id,
            distr_poezdki_po_gorodam.poezdok,
            distr_poezdki_po_gorodam.poezdok / SUM(IFNULL(distr_poezdki_po_gorodam.poezdok,0)) OVER (PARTITION BY distr_poezdki_po_gorodam.start_time) AS 'coef_goroda'
        FROM 
            (SELECT 
                DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d') AS  'start_time',
                t_bike.city_id,
                COUNT(t_bike_use.ride_amount) AS 'poezdok'
            FROM shamri.t_bike_use
            LEFT JOIN t_bike ON t_bike_use.bid = t_bike.id
            WHERE t_bike_use.ride_status!=5 
                AND DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d') >= '2024-07-21'
            GROUP BY DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d'), t_bike.city_id
            ORDER BY DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d') DESC) 
            AS distr_poezdki_po_gorodam
        ORDER BY distr_poezdki_po_gorodam.start_time DESC
        ) AS distr_poezdki_po_gorodam
        LEFT JOIN (
            SELECT 
                DATE_FORMAT(t_subscription_mapping.start_time, '%Y-%m-%d') AS start_time,
                SUM(IFNULL(t_subscription.price,0)) AS sum_mnogor_abon
            FROM t_subscription_mapping
            LEFT JOIN t_subscription ON t_subscription_mapping.subscription_id = t_subscription.id
            WHERE DATE_FORMAT(t_subscription_mapping.start_time, '%Y-%m-%d') >= '2024-07-21'
            GROUP BY DATE_FORMAT(t_subscription_mapping.start_time, '%Y-%m-%d')
            ORDER BY DATE_FORMAT(t_subscription_mapping.start_time, '%Y-%m-%d') DESC
            ) AS sum_mnogor_abon
        ON distr_poezdki_po_gorodam.start_time = sum_mnogor_abon.start_time
        ORDER BY distr_poezdki_po_gorodam.start_time DESC
    ),
    kvt AS (
        SELECT 
            DATE_FORMAT(FROM_UNIXTIME(t_bike.heart_time), '%Y-%m-%d') AS start_time,
            t_bike.city_id,
            COUNT(t_bike.id) AS kvt
        FROM t_bike
        WHERE TIMESTAMPDIFF(SECOND, now(), DATE_FORMAT(FROM_UNIXTIME(t_bike.g_time), '%Y-%m-%d %H:%m:%s')) < 900 
            AND t_bike.error_status IN (0, 7) 
        GROUP BY DATE_FORMAT(FROM_UNIXTIME(t_bike.heart_time), '%Y-%m-%d'), t_bike.city_id
        HAVING start_time = DATE_FORMAT(NOW(), '%Y-%m-%d')
        ORDER BY t_bike.city_id DESC
    ),
    vyruchka_uspeh_payTabs AS 
    (
        SELECT
            vyruchka_uspeh_payTabs.start_time,
            vyruchka_uspeh_payTabs.city_id,
            vyruchka_uspeh_payTabs.vyruchka_v_statuse_1_payTabs,
            chastichno_vozvrascheny.chastichno_vozvrascheny AS 'chastichno_vozvrascheny'
        FROM
            (SELECT 
                DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') AS start_time,
                t_trade.city_id,
                SUM(IFNULL(t_trade.account_pay_amount,0)) AS 'vyruchka_v_statuse_1_payTabs'
            FROM t_trade
            WHERE t_trade.status=1 
                AND t_trade.way=26 
                AND t_trade.`type` IN (1,2,6,7)
                AND DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') >= '2024-07-21'
            GROUP BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d'), t_trade.city_id
            ORDER BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') DESC, t_trade.city_id DESC
            ) AS vyruchka_uspeh_payTabs
        LEFT JOIN 
            (
            SELECT
                DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') AS start_time,
                t_trade.city_id,
                SUM(IFNULL(t_trade.account_pay_amount,0)) AS 'chastichno_vozvrascheny'
            FROM t_trade
            WHERE t_trade.status=3
                AND DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') >= '2024-07-21'
            GROUP BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d'), t_trade.city_id
            ) AS chastichno_vozvrascheny ON vyruchka_uspeh_payTabs.start_time = chastichno_vozvrascheny.start_time AND vyruchka_uspeh_payTabs.city_id = chastichno_vozvrascheny.city_id
    ),
    vyruchka_uspeh_payTabs_ni AS 
    (
         SELECT
             vyruchka_uspeh_payTabs.city_id,
             vyruchka_uspeh_payTabs.start_time, 
             IFNULL(vyruchka_uspeh_payTabs.chastichno_vozvrascheny,0) AS 'chastichno_vozvrascheny',
            vyruchka_uspeh_payTabs.vyruchka_v_statuse_1_payTabs_ni AS 'vyruchka_v_statuse_1_payTabs_ni'
         FROM 
            (
            SELECT
                vyruchka_uspeh_payTabs.start_time,
                vyruchka_uspeh_payTabs.city_id,
                vyruchka_uspeh_payTabs.vyruchka_v_statuse_1_payTabs,
                chastichno_vozvrascheny.chastichno_vozvrascheny,
                SUM(IFNULL(vyruchka_uspeh_payTabs.vyruchka_v_statuse_1_payTabs,0) + IFNULL(chastichno_vozvrascheny.chastichno_vozvrascheny,0)) OVER (PARTITION BY vyruchka_uspeh_payTabs.city_id ORDER BY vyruchka_uspeh_payTabs.start_time) AS 'vyruchka_v_statuse_1_payTabs_ni'
            FROM
                (SELECT 
                    DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') AS start_time,
                    t_trade.city_id,
                    SUM(IFNULL(t_trade.account_pay_amount,0)) AS 'vyruchka_v_statuse_1_payTabs'
                FROM t_trade
                WHERE t_trade.status=1 
                    AND t_trade.way=26 
                    AND t_trade.`type` IN (1,2,6,7) 
                    AND DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') >= '2024-07-21' 
                GROUP BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d'), t_trade.city_id
                ORDER BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') DESC, t_trade.city_id DESC) AS vyruchka_uspeh_payTabs
            LEFT JOIN 
            (
                SELECT
                    DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') AS start_time,
                    t_trade.city_id,
                    SUM(IFNULL(t_trade.account_pay_amount,0)) AS 'chastichno_vozvrascheny'
                FROM t_trade
                WHERE t_trade.status=3
                    AND DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') >= '2024-07-21'
                GROUP BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d'), t_trade.city_id
                ) AS chastichno_vozvrascheny ON vyruchka_uspeh_payTabs.start_time = chastichno_vozvrascheny.start_time AND vyruchka_uspeh_payTabs.city_id = chastichno_vozvrascheny.city_id
                )
            AS vyruchka_uspeh_payTabs
    ),
    vosvraty_payTabs AS ( 
        SELECT
            trade.start_time,
            trade.city_id,
            trade.vozvraty_payTabs,
            SUM(IFNULL(trade.vozvraty_payTabs,0)) OVER (PARTITION BY trade.city_id ORDER BY trade.start_time) AS 'vozvraty_payTabs_ni'
        FROM
            (SELECT
                DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') AS start_time,
                t_trade.city_id,
                SUM(IFNULL(t_trade.account_pay_amount,0)) AS 'vozvraty_payTabs'
            FROM t_trade
            WHERE t_trade.status=4 
                 AND t_trade.way=26 
                 AND DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') >= '2024-07-21'
            GROUP BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d'), t_trade.city_id
            ORDER BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') DESC) AS trade
     ),
     vozvraty_payTabs_ni AS (
         SELECT
             vozvraty_payTabs.city_id,
             vozvraty_payTabs.start_time,
             vozvraty_payTabs.vozvraty_payTabs_ni
         FROM 
            (SELECT
                trade.start_time,
                trade.city_id,
                trade.vozvraty_payTabs,
                SUM(IFNULL(trade.vozvraty_payTabs,0)) OVER (PARTITION BY trade.city_id ORDER BY trade.start_time) AS 'vozvraty_payTabs_ni'
            FROM
                (SELECT
                    DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') AS start_time,
                    t_trade.city_id,
                    SUM(IFNULL(t_trade.account_pay_amount, 0)) AS 'vozvraty_payTabs'
                FROM t_trade
                WHERE t_trade.status=4 
                     AND t_trade.way=26 
                     AND DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') >= '2024-07-21'
                GROUP BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d'), t_trade.city_id
                ORDER BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') DESC) AS trade) AS vozvraty_payTabs
     ),
    uspeh_Stripe AS (
        SELECT
            trade.start_time,
            trade.city_id,
            trade.uspeh_Stripe,
            SUM(IFNULL(trade.uspeh_Stripe, 0)) OVER (PARTITION BY trade.city_id ORDER BY trade.start_time) AS 'uspeh_Stripe_ni'
        FROM 
            (SELECT 
                DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') AS start_time,
                t_trade.city_id,
                SUM(IFNULL(t_trade.account_pay_amount,0)) AS 'uspeh_Stripe'
            FROM t_trade
            WHERE t_trade.status=1 
                 AND t_trade.way=6 
                 AND DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') > '2024-07-21'
            GROUP BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d'), t_trade.city_id
            ORDER BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') DESC) AS trade
     ),
     uspeh_Stripe_ni AS(
         SELECT 
             uspeh_Stripe.city_id,
             uspeh_Stripe.start_time,
            uspeh_Stripe.uspeh_Stripe,
            uspeh_Stripe.uspeh_Stripe_ni
         FROM 
            (SELECT
                trade.start_time,
                trade.city_id,
                trade.uspeh_Stripe,
                SUM(IFNULL(trade.uspeh_Stripe, 0)) OVER (PARTITION BY trade.city_id ORDER BY trade.start_time) AS 'uspeh_Stripe_ni'
            FROM 
                (SELECT 
                    DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') AS start_time,
                    t_trade.city_id,
                    SUM(t_trade.account_pay_amount) AS 'uspeh_Stripe'
                FROM t_trade
                WHERE t_trade.status=1 
                     AND t_trade.way=6 
                     AND DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') >= '2024-07-21'
                GROUP BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d'), t_trade.city_id
                ORDER BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') DESC) AS trade) 
            AS uspeh_Stripe
     ),
     vozvraty_Stripe AS (
     -- Города - ок
        SELECT
            trade.start_time,
            trade.city_id,
            trade.vozvraty_Stripe,
            SUM(IFNULL(trade.vozvraty_Stripe, 0)) OVER (PARTITION BY trade.city_id ORDER BY trade.start_time) AS 'vozvraty_Stripe_ni'
        FROM
            (SELECT 
                DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') AS start_time,
                t_trade.city_id,
                SUM(t_trade.account_pay_amount) AS 'vozvraty_Stripe'
             FROM t_trade
             WHERE t_trade.status=4 
                 AND t_trade.way=6 
                 AND DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') >= '2024-07-21'
             GROUP BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d'), t_trade.city_id
             ORDER BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') DESC) AS trade
     ),
     vozvraty_Stripe_ni AS (
         SELECT
             vozvraty_Stripe.city_id,
             vozvraty_Stripe.start_time,
             vozvraty_Stripe.vozvraty_Stripe_ni
         FROM 
            (SELECT
                trade.start_time,
                trade.city_id,
                trade.vozvraty_Stripe,
                SUM(IFNULL(trade.vozvraty_Stripe, 0)) OVER (PARTITION BY trade.city_id ORDER BY trade.start_time) AS 'vozvraty_Stripe_ni'
            FROM
                (SELECT 
                    DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') AS start_time,
                    t_trade.city_id,
                    SUM(IFNULL(t_trade.account_pay_amount,0)) AS 'vozvraty_Stripe'
                 FROM t_trade
                 WHERE t_trade.status=4 
                     AND t_trade.way=6 
                     AND DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') >= '2024-07-21'
                 GROUP BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d'), t_trade.city_id
                 ORDER BY DATE_FORMAT(t_trade.`date`, '%Y-%m-%d') DESC) AS trade)
            AS vozvraty_Stripe
     ),
    user_v_den_register AS (
        SELECT
            distr.start_date AS 'start_time',
            distr.city_id,
            distr.coef_goroda * user_v_den_register.user_v_den_register AS 'user_v_den_register',
            SUM(IFNULL(distr.coef_goroda * user_v_den_register.user_v_den_register,0)) OVER (PARTITION BY distr.city_id ORDER BY distr.start_date) AS 'user_v_den_register_ni',
            user_v_den_register.user_v_den_register AS 'user_v_den_register_vsego',
            distr.kolichestvo_novyh_s_1_poezdkoy,
            distr.kolichestvo_novyh_s_1_poezdkoy_vsego,
            distr.coef_goroda    
        FROM 
            (
            SELECT
                register_date_as_start_date.start_date,
                register_date_as_start_date.city_id,
                COUNT(DISTINCT register_date_as_start_date.uid) AS 'kolichestvo_novyh_s_1_poezdkoy',
                SUM(COUNT(DISTINCT register_date_as_start_date.uid)) OVER (PARTITION BY register_date_as_start_date.start_date) AS 'kolichestvo_novyh_s_1_poezdkoy_vsego',
                COUNT(DISTINCT register_date_as_start_date.uid) / SUM(COUNT(DISTINCT register_date_as_start_date.uid)) OVER (PARTITION BY register_date_as_start_date.start_date) AS 'coef_goroda'
            FROM
                (SELECT 
                    DATE(DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d')) AS start_date, 
                    t_bike_use.uid,
                    t_bike_use.ride_amount,
                    register_users.city_id,
                    register_users.register_date,
                    register_users.id AS register_user_id
                FROM t_bike_use
                LEFT JOIN (
                    SELECT 
                        DATE(DATE_FORMAT(t_user.register_date, '%Y-%m-%d')) AS register_date, 
                        t_user.id,
                        t_user.city_id
                    FROM t_user
                                    ) AS register_users
                ON t_bike_use.uid=register_users.id
                WHERE DATE(DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d')) = DATE(DATE_FORMAT(register_users.register_date, '%Y-%m-%d'))
                    AND DATE(DATE_FORMAT(register_users.register_date, '%Y-%m-%d')) >= '2024-07-21'
                    AND DATE(DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d')) >= '2024-07-21'
                ) AS register_date_as_start_date
            GROUP BY register_date_as_start_date.start_date, register_date_as_start_date.city_id
            ORDER BY register_date_as_start_date.start_date DESC
            ) AS distr
        LEFT JOIN (
            SELECT
                user_register.start_time,
                user_register.user_v_den_register
            FROM 
                (SELECT 
                    DATE_FORMAT(t_user.register_date, '%Y-%m-%d') AS start_time,
                    COUNT(t_user.id) AS 'user_v_den_register'
                FROM t_user
                WHERE DATE_FORMAT(t_user.register_date, '%Y-%m-%d') >= '2024-07-21'
                GROUP BY DATE_FORMAT(t_user.register_date, '%Y-%m-%d')
                ORDER BY DATE_FORMAT(t_user.register_date, '%Y-%m-%d') DESC
                    ) AS user_register
            ORDER BY user_register.start_time DESC
        ) AS user_v_den_register
        ON distr.start_date = user_v_den_register.start_time
        ORDER BY distr.start_date DESC
    ),
    kolichestvo_novyh_s_1_poezdkoy AS ( 
        SELECT
            register_date_as_start_date.start_date,
            register_date_as_start_date.city_id,
            COUNT(DISTINCT register_date_as_start_date.uid) AS 'kolichestvo_novyh_s_1_poezdkoy',
            COUNT(register_date_as_start_date.ride_amount) AS 'kolichestvo_poezdok_vsego'
        FROM
            (SELECT 
                DATE(DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d')) AS start_date, 
                t_bike_use.uid,
                t_bike_use.ride_amount,
                register_users.city_id,
                register_users.register_date,
                register_users.id AS register_user_id
            FROM t_bike_use
            LEFT JOIN (
                SELECT 
                    DATE(DATE_FORMAT(t_user.register_date, '%Y-%m-%d')) AS register_date, 
                    t_user.id,
                    t_user.city_id
                FROM t_user
                                ) AS register_users
            ON t_bike_use.uid=register_users.id
            WHERE DATE(DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d')) = DATE(DATE_FORMAT(register_users.register_date, '%Y-%m-%d'))
                AND DATE(DATE_FORMAT(register_users.register_date, '%Y-%m-%d')) >= '2024-07-21'
                AND DATE(DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d')) >= '2024-07-21'
                AND t_bike_use.ride_status!=5
            ) AS register_date_as_start_date
        GROUP BY register_date_as_start_date.start_date, register_date_as_start_date.city_id
    ),
    dolgi AS (
        SELECT 
            dolgi.create_debit_date,
            dolgi.city_id,
            dolgi.dolgi,
            SUM(IFNULL(dolgi.dolgi,0)) OVER (PARTITION BY dolgi.city_id ORDER BY dolgi.create_debit_date) AS 'dolgi_ni'
        FROM 
            (SELECT 
                DATE_FORMAT(t_payment_details.created, '%Y-%m-%d') AS 'create_debit_date', 
                dolgovye_poezdki.city_id,
                SUM(IFNULL(t_payment_details.debit_cash,0)) AS 'dolgi'
            FROM t_payment_details
            RIGHT JOIN (
                    SELECT 
                        t_bike_use.id,
                        t_bike_use.uid,
                        t_bike_use.bid,
                        t_bike.city_id
                    FROM t_bike_use
                    LEFT JOIN t_bike ON t_bike_use.bid = t_bike.id
                    WHERE t_bike_use.ride_status = 2 
                        AND DATE(DATE_FORMAT(FROM_UNIXTIME(t_bike_use.start_time), '%Y-%m-%d')) >= '2024-07-21'
                        ) AS dolgovye_poezdki
            ON t_payment_details.user_id = dolgovye_poezdki.uid AND t_payment_details.ride_id = dolgovye_poezdki.id
            WHERE DATE_FORMAT(t_payment_details.created, '%Y-%m-%d') >= '2024-07-21'
            GROUP BY DATE_FORMAT(t_payment_details.created, '%Y-%m-%d'), dolgovye_poezdki.city_id
            ORDER BY DATE_FORMAT(t_payment_details.created, '%Y-%m-%d') DESC
            ) AS dolgi
    ),
    t_city_with_noname AS (
        SELECT
            calendar.gen_date AS 'start_day',
            t_city_with_noname.id,
            t_city_with_noname.name
        FROM (
        select * from 
            (select adddate('1970-01-01',t4*10000 + t3*1000 + t2*100 + t1*10 + t0) gen_date from
             (select 0 t0 union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t0,
             (select 0 t1 union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t1,
             (select 0 t2 union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t2,
             (select 0 t3 union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t3,
             (select 0 t4 union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t4) v
            where gen_date between '2024-07-21' and DATE_FORMAT(NOW(), '%Y-%m-%d')
        ) AS calendar
        CROSS JOIN (
            SELECT 
                t_city.*
            FROM t_city
            UNION
            SELECT 
                0 AS 'id', 
                'Unknown' AS 'name', 
                '' AS 'note', 
                NULL AS 'code', 
                '' AS 'area_detail', 
                0 AS 'area_lat', 
                0 AS 'area_lng', 
                NULL AS 'currency', 
                '00:00' AS 'start_time', 
                '00:24' AS 'end_time', 
                NULL AS 'invite_code', 
                '{"max_speed_limit":0}' AS 'extend_info', 
                1 AS 'industry_id'
        ) AS t_city_with_noname
    )
    SELECT 
        NOW() AS 'timestamp',
        -- DATE_FORMAT(t_city_with_noname.start_day, '%Y-%m-%d %H:%m:%s') AS 'day_',
        t_city_with_noname.id,
        t_city_with_noname.name,
        IFNULL(three_left_cols.poezdok, 0)  AS 'poezdok',
        IFNULL(three_left_cols.poezdok, 0) / IFNULL(kvt.kvt, 0) AS 'poezdok_v_srednem_na_samokat',
        IFNULL((IFNULL(three_left_cols.obzchaya_stoimost,0) - IFNULL(sum_mnogor_abon.sum_mnogor_abon,0) - IFNULL(three_left_cols.oplacheno_bonusami,0) + IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0)),0) / IFNULL(kvt.kvt,0) AS 'vyruchka_sim',
        (IFNULL(three_left_cols.obzchaya_stoimost,0) - IFNULL(sum_mnogor_abon.sum_mnogor_abon,0) - IFNULL(three_left_cols.oplacheno_bonusami,0) + IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0)) / IFNULL(three_left_cols.poezdok,0) AS 'srednyaa_cena_poezdki',
        IFNULL(three_left_cols.oplacheno_bonusami,0) AS 'oplacheno_bonusami',
        IFNULL(three_left_cols.obzchaya_stoimost,0) - IFNULL(dolgi.dolgi,0) - (IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0) - IFNULL(sum_mnogor_abon.sum_mnogor_abon,0)) AS 'vyruchka',
        IFNULL(three_left_cols.obzchaya_stoimost,0) - IFNULL(dolgi.dolgi,0) - (IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0) - IFNULL(sum_mnogor_abon.sum_mnogor_abon,0)) - IFNULL(three_left_cols.oplacheno_bonusami,0) AS 'vyruchka_bez_bonusov',
        IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0) AS 'vyruchka_s_abonementov',
        IFNULL(three_left_cols.obzchaya_stoimost,0) - IFNULL(dolgi.dolgi,0) - (IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0) - IFNULL(sum_mnogor_abon.sum_mnogor_abon,0)) - IFNULL(three_left_cols.oplacheno_bonusami,0) + IFNULL(sum_uspeh_abon.vyruchka_s_abonementov,0) AS 'vyruchka_bez_bonusov_vyruchka_s_abonementov',
        IFNULL(three_left_cols.obzchaya_stoimost_ni,0) - IFNULL(dolgi.dolgi_ni,0) - IFNULL(sum_uspeh_abon.vyruchka_s_abonementov_ni,0) + IFNULL(sum_mnogor_abon.sum_mnogor_abon_ni,0) + IFNULL(sum_uspeh_abon.vyruchka_s_abonementov_ni,0) AS 'vni',
        IFNULL(three_left_cols.obzchaya_stoimost_ni,0) - IFNULL(dolgi.dolgi_ni,0) - IFNULL(sum_uspeh_abon.vyruchka_s_abonementov_ni,0) + IFNULL(sum_mnogor_abon.sum_mnogor_abon_ni,0) + IFNULL(sum_uspeh_abon.vyruchka_s_abonementov_ni,0) - IFNULL(three_left_cols.oplacheno_bonusami_ni,0) AS 'vni_bez_bonusov',
        IFNULL(kvt.kvt, 0) AS 'kvt',
        IFNULL(user_v_den_register.user_v_den_register, 0) AS 'user_v_den_register',
        IFNULL(user_v_den_register.user_v_den_register_ni, 0) AS 'user_v_den_obshc',
        IFNULL(three_left_cols.obschee_vremya_min, 0) AS 'obcshee_vrmeya_min',
        IFNULL(three_left_cols.obschee_vremya_min, 0) / IFNULL(three_left_cols.poezdok, 0) AS 'srednyee_vremya_poezdki',
        IFNULL(kolichestvo_novyh_s_1_poezdkoy.kolichestvo_novyh_s_1_poezdkoy, 0) AS 'iz_nih_usero_sovershivshih_poezdki_vsego',
        IFNULL(kolichestvo_novyh_s_1_poezdkoy.kolichestvo_poezdok_vsego, 0) AS 'kol_vo_poezdok_vsego',
        ROUND((kolichestvo_novyh_s_1_poezdkoy.kolichestvo_novyh_s_1_poezdkoy / user_v_den_register.user_v_den_register) * 100, 2) AS 'proniknovenie',
        ROUND(kolichestvo_novyh_s_1_poezdkoy.kolichestvo_poezdok_vsego / kolichestvo_novyh_s_1_poezdkoy.kolichestvo_novyh_s_1_poezdkoy, 2) AS 'poezdok_novyi_user'
    FROM t_city_with_noname
    LEFT JOIN kvt ON t_city_with_noname.start_day = kvt.start_time AND t_city_with_noname.id = kvt.city_id
    LEFT JOIN sum_uspeh_abon ON t_city_with_noname.start_day = sum_uspeh_abon.start_time AND t_city_with_noname.id = sum_uspeh_abon.city_id
    LEFT JOIN sum_mnogor_abon ON t_city_with_noname.start_day = sum_mnogor_abon.start_time AND t_city_with_noname.id = sum_mnogor_abon.city_id
    LEFT JOIN vyruchka_uspeh_payTabs ON t_city_with_noname.start_day = vyruchka_uspeh_payTabs.start_time AND t_city_with_noname.id = vyruchka_uspeh_payTabs.city_id
    LEFT JOIN vyruchka_uspeh_payTabs_ni ON t_city_with_noname.start_day = vyruchka_uspeh_payTabs_ni.start_time AND t_city_with_noname.id = vyruchka_uspeh_payTabs_ni.city_id
    LEFT JOIN vosvraty_payTabs ON t_city_with_noname.start_day = vosvraty_payTabs.start_time AND t_city_with_noname.id = vosvraty_payTabs.city_id
    LEFT JOIN vozvraty_payTabs_ni ON t_city_with_noname.start_day = vozvraty_payTabs_ni.start_time AND t_city_with_noname.id = vozvraty_payTabs_ni.city_id
    LEFT JOIN uspeh_Stripe ON t_city_with_noname.start_day = uspeh_Stripe.start_time AND t_city_with_noname.id = uspeh_Stripe.city_id
    LEFT JOIN uspeh_Stripe_ni ON t_city_with_noname.start_day = uspeh_Stripe_ni.start_time AND t_city_with_noname.id = uspeh_Stripe_ni.city_id
    LEFT JOIN vozvraty_Stripe ON t_city_with_noname.start_day = vozvraty_Stripe.start_time AND t_city_with_noname.id = vozvraty_Stripe.city_id
    LEFT JOIN vozvraty_Stripe_ni ON t_city_with_noname.start_day = vozvraty_Stripe_ni.start_time AND t_city_with_noname.id = vozvraty_Stripe_ni.city_id
    LEFT JOIN user_v_den_register ON t_city_with_noname.start_day = user_v_den_register.start_time AND t_city_with_noname.id = user_v_den_register.city_id 
    LEFT JOIN kolichestvo_novyh_s_1_poezdkoy ON t_city_with_noname.start_day = kolichestvo_novyh_s_1_poezdkoy.start_date AND t_city_with_noname.id = kolichestvo_novyh_s_1_poezdkoy.city_id
    LEFT JOIN dolgi ON t_city_with_noname.start_day = dolgi.create_debit_date AND t_city_with_noname.id = dolgi.city_id
    LEFT JOIN three_left_cols ON t_city_with_noname.start_day = three_left_cols.start_time AND t_city_with_noname.id = three_left_cols.city_id
    WHERE IFNULL(three_left_cols.poezdok, 0) > 0
    AND t_city_with_noname.start_day = DATE_FORMAT(NOW(), '%Y-%m-%d')
    ORDER BY t_city_with_noname.start_day DESC
    '''

    df_cities = pd.read_sql(select_vni_cities, engine_mysql)
    df_cities.to_sql("vni_cities", engine_postgresql, if_exists="append", index=False)

    print('Got it: VNI_cities')

    # Выгрузка по городам для графиков
    select_vni_cities_for_graph = '''
    SELECT *
    FROM (
        SELECT
            date(vni_cities.timestamp),
            vni_cities.*,
            rank() OVER (PARTITION BY vni_cities.id, date(vni_cities.timestamp) ORDER BY vni_cities.timestamp DESC) AS rank
        FROM vni_cities
        ORDER BY vni_cities."timestamp" DESC
        ) AS ranked
    WHERE ranked."rank" = 1 
    '''

    df_vni_cities_for_graph = pd.read_sql(select_vni_cities_for_graph, engine_postgresql)

    # Очистка таблиц
    truncate_vni_cities_for_graph = "TRUNCATE TABLE vni_cities_for_graph RESTART IDENTITY;"
    truncate_t_bike = "TRUNCATE TABLE t_bike RESTART IDENTITY;"
    truncate_t_city = "TRUNCATE TABLE t_city RESTART IDENTITY;"

    with engine_postgresql.connect() as connection:
        with connection.begin() as transaction:
            print(f"Попытка очистить таблицы")
            connection.execute(sa.text(truncate_vni_cities_for_graph))
            # Очистка t_bike
            connection.execute(sa.text(truncate_t_bike))
            # Очистка t_city
            connection.execute(sa.text(truncate_t_city))
            # Если ошибок нет, транзакция фиксируется автоматически
            print(f"Таблица vni_cities_for_graph успешно очищена.")

    # Загрузка таблицы для графиков
    df_vni_cities_for_graph.to_sql("vni_cities_for_graph", engine_postgresql, if_exists="append", index=False)
    print('Table for graphs vni_cities_for_graph updated!')

    # Копирую t_bike
    select_t_bike = '''	SELECT
	NOW() as 'timestamp',
	IFNULL(t_bike.id,0) AS id,
	IFNULL(t_bike.number,0) AS number,
	IFNULL(t_bike.imei,0) AS imei,
	IFNULL(t_bike.type_id,0) AS type_id,
	IFNULL(t_bike.g_time,0) AS g_time,
	IFNULL(t_bike.g_lat,0) AS g_lat,
	IFNULL(t_bike.g_lng,0) AS g_lng,
	IFNULL(t_bike.status,0) AS status,
	IFNULL(t_bike.use_status,0) AS use_status,
	IFNULL(t_bike.power,0) AS power,
	IFNULL(t_bike.gsm,0) AS gsm,
	IFNULL(t_bike.gps_number,'empty') AS gps_number,
	IFNULL(t_bike.city_id,0) AS city_id,
	IFNULL(t_bike.heart_time,0) AS heart_time,
	IFNULL(t_bike.version,0) AS version,
	IFNULL(t_bike.version_time,0) AS version_time,
	IFNULL(t_bike.readpack,0) AS readpack,
	IFNULL(t_bike.add_date, STR_TO_DATE("2024-01-01 00:00:00", "%Y-%m-%d %H:%i:%s")) AS add_date,
	IFNULL(t_bike.error_status,0) AS error_status,
	IFNULL(t_bike.server_ip,'0.0.0.0') AS server_ip,
	IFNULL(t_bike.bike_status,0) AS bike_status,
	IFNULL(t_bike.sponsors_id,0) AS sponsors_id,
	IFNULL(t_bike.bike_no,0) AS bike_no,
	IFNULL(t_bike.bike_type,0) AS bike_type,
	IFNULL(t_bike.extend_info,0) AS extend_info,
	IFNULL(t_bike.area_id,0) AS area_id,
	IFNULL(t_bike.bike_power,0) AS bike_power,
	IFNULL(t_bike.bike_power_status,0) AS bike_power_status,
	IFNULL(t_bike.mac,0) AS mac,
	IFNULL(t_bike.iccid,'empty') AS iccid,
	IFNULL(t_bike.maintain_status,0) AS maintain_status,
	IFNULL(t_bike.extra_lock_status,0) AS extra_lock_status,
	IFNULL(t_bike.available,0) AS available,
	IFNULL(t_bike.model,'empty') AS model,
	IFNULL(t_bike.protocol,0) AS protocol,
	IFNULL(t_bike.frame_number,'empty') AS frame_number,
	IFNULL(t_bike.battery_key,0) AS battery_key,
	IFNULL(t_bike.release_time, STR_TO_DATE("2024-01-01 00:00:00", "%Y-%m-%d %H:%i:%s")) AS release_time,
	IFNULL(t_bike.last_service_time, STR_TO_DATE("2024-01-01 00:00:00", "%Y-%m-%d %H:%i:%s")) AS last_service_time,
	IFNULL(t_bike.industry_id,0) AS industry_id,
	IFNULL(t_bike.ble_key,'empty') AS ble_key,
	IFNULL(t_bike.user_group_id,0) AS user_group_id FROM shamri.t_bike'''
    df_t_bike = pd.read_sql(select_t_bike, engine_mysql)

    select_t_city = '''	SELECT
    	NOW() as 'timestamp',
    	IFNULL(t_city.id,0) AS id,
    	IFNULL(t_city.name,'empty') AS name,
    	IFNULL(t_city.note,'City zone') AS note,
    	IFNULL(t_city.code,0) AS code,
    	IFNULL(t_city.area_detail,'Empty') AS area_detail,
    	IFNULL(t_city.area_lat,0) AS area_lat,
    	IFNULL(t_city.area_lng,0) AS area_lng,
    	IFNULL(t_city.currency,'Empty') AS currency,
    	IFNULL(t_city.start_time,'00:00') AS start_time,
    	IFNULL(t_city.end_time,'00:24') AS end_time,
    	IFNULL(t_city.invite_code,'Empty') AS invite_code,
    	IFNULL(t_city.extend_info,'Empty') AS extend_info,
    	IFNULL(t_city.industry_id,0) AS industry_id FROM shamri.t_city
    '''
    df_t_city = pd.read_sql(select_t_city, engine_mysql)




    # Загрузка t_bike
    df_t_bike.to_sql("t_bike", engine_postgresql, if_exists="append", index=False)
    print('Table for graphs t_bike!')

    # Загрузка t_city
    df_t_city.to_sql("t_city", engine_postgresql, if_exists="append", index=False)
    print('Table for graphs t_city!')



    # АКБ - начало
    select_df1 = '''
    -- Собираю из БД MYSQL
    WITH uteryany AS (
    	SELECT
    		t_bike.city_id,
    	    t_bike.error_status,
    	    COUNT(t_bike.id) AS uteryany
    	FROM
    	    t_bike
    	WHERE t_bike.error_status = 6 AND t_bike.bike_type = 2 
        AND t_bike.model LIKE '%freego%'
    	GROUP BY
    		t_bike.city_id, t_bike.error_status
    ),
    v_ozhidanii_activacii AS (
    	SELECT
    		t_bike.city_id,
    	    t_bike.error_status,
    	    COUNT(t_bike.id) AS v_ozhidanii_activacii
    	FROM
    	    t_bike
    	WHERE t_bike.error_status = 4 AND t_bike.bike_type = 2 
        -- AND t_bike.model LIKE '%freego%'
    	GROUP BY
    		t_bike.city_id, t_bike.error_status
    ),
    slugebnyi_transport AS (
    	SELECT
    		t_bike.city_id,
    	    t_bike.error_status,
    	    COUNT(t_bike.id) AS slugebnyi_transport
    	FROM
    	    t_bike
    	WHERE t_bike.error_status = 8 AND t_bike.bike_type = 2 
        -- AND t_bike.model LIKE '%freego%'
    	GROUP BY
    		t_bike.city_id, t_bike.error_status
    ),
    remont AS (
    	SELECT
    		t_bike.city_id,
    	    t_bike.error_status,
    	    COUNT(t_bike.id) AS remont
    	FROM
    	    t_bike
    	WHERE t_bike.error_status = 5 AND t_bike.bike_type = 2 
        -- AND t_bike.model LIKE '%freego%'
    	GROUP BY
    		t_bike.city_id, t_bike.error_status
    ),
    vyveden_iz_ekspluatacii AS (
    	SELECT
    		t_bike.city_id,
    	    t_bike.error_status,
    	    COUNT(t_bike.id) AS vyveden_iz_ekspluatacii
    	FROM
    	    t_bike
    	WHERE t_bike.error_status = 3 AND t_bike.bike_type = 2 
        -- AND t_bike.model LIKE '%freego%'
    	GROUP BY
    		t_bike.city_id, t_bike.error_status
    ),
    mr_user AS (
    	SELECT
    		t_bike.city_id,
    	    t_bike.error_status,
    	    COUNT(t_bike.id) AS mr_user
    	FROM
    	    t_bike
    	WHERE t_bike.error_status = 2 AND t_bike.bike_type = 2 
        -- AND t_bike.model LIKE '%freego%'
    	GROUP BY
    		t_bike.city_id, t_bike.error_status
    ),
    kvt AS (
    	SELECT
    	    t_bike.city_id,
    	    COUNT(t_bike.id) AS kvt
    	FROM
    	    t_bike
    	WHERE
    	    TIMESTAMPDIFF(SECOND, FROM_UNIXTIME(t_bike.heart_time), NOW()) < 900
    	    AND t_bike.error_status IN (0, 7)
    	    AND t_bike.bike_type = 2
    	    -- AND t_bike.model LIKE '%freego%'
    	GROUP BY
    	    t_bike.city_id
    	ORDER BY
    	    t_bike.city_id DESC
    ),
    kvt_offline AS (
    	SELECT
    	    t_bike.city_id,
    	    COUNT(t_bike.id) AS kvt_offline
    	FROM
    	    t_bike
    	WHERE
    	    TIMESTAMPDIFF(SECOND, FROM_UNIXTIME(t_bike.heart_time), NOW()) > 900
    	    AND t_bike.error_status IN (0, 7)
    	    AND t_bike.bike_type = 2
    	    -- AND t_bike.model LIKE '%freego%'
    	GROUP BY
    	    t_bike.city_id
    	ORDER BY
    	    t_bike.city_id DESC
    ),
    mr_admin AS (
    	SELECT
    		t_bike.city_id,
    	    t_bike.error_status,
    	    COUNT(t_bike.id) AS mr_admin
    	FROM
    	    t_bike
    	WHERE t_bike.error_status = 1 
        AND t_bike.bike_type = 2 
        -- AND t_bike.model LIKE '%freego%'
    	GROUP BY
    		t_bike.city_id, t_bike.error_status
    )
    SELECT
    	t_city.id AS city_id,
    	t_city.name,
    	COALESCE(uteryany.uteryany,0) AS uteryany,
    	COALESCE(v_ozhidanii_activacii.v_ozhidanii_activacii,0) AS v_ozhidanii_activacii,
    	COALESCE(slugebnyi_transport.slugebnyi_transport,0) AS slugebnyi_transport,
    	COALESCE(remont.remont,0) AS remont,
    	COALESCE(vyveden_iz_ekspluatacii.vyveden_iz_ekspluatacii,0) AS vyveden_iz_ekspluatacii,
    	COALESCE(mr_user.mr_user,0) AS mr_user,
    	COALESCE(kvt.kvt,0) AS kvt,
    	COALESCE(mr_admin.mr_admin,0) AS mr_admin,
    	COALESCE(mr_admin.mr_admin,0) + COALESCE(kvt.kvt,0) AS fact,
    	COALESCE(uteryany.uteryany,0) + COALESCE(v_ozhidanii_activacii.v_ozhidanii_activacii,0) + COALESCE(slugebnyi_transport.slugebnyi_transport,0) + COALESCE(remont.remont,0) + COALESCE(vyveden_iz_ekspluatacii.vyveden_iz_ekspluatacii,0) + COALESCE(mr_user.mr_user,0) + COALESCE(kvt.kvt,0) + COALESCE(mr_admin.mr_admin,0) AS itogo_sim,
    	COALESCE(kvt_offline.kvt_offline,0) AS kvt_offline
    FROM t_city
    LEFT JOIN uteryany ON t_city.id = uteryany.city_id
    LEFT JOIN v_ozhidanii_activacii ON t_city.id = v_ozhidanii_activacii.city_id
    LEFT JOIN slugebnyi_transport ON t_city.id = slugebnyi_transport.city_id
    LEFT JOIN remont ON t_city.id = remont.city_id
    LEFT JOIN vyveden_iz_ekspluatacii ON t_city.id = vyveden_iz_ekspluatacii.city_id
    LEFT JOIN mr_user ON t_city.id = mr_user.city_id
    LEFT JOIN kvt ON t_city.id = kvt.city_id
    LEFT JOIN kvt_offline ON t_city.id = kvt_offline.city_id
    LEFT JOIN mr_admin ON t_city.id = mr_admin.city_id
    '''
    df1 = pd.read_sql(select_df1, engine_mysql)

    select_h119 = '''
    SELECT
        t_bike.error_status,
        COUNT(t_bike.id) AS vyveden_iz_ekspluatacii
    FROM
        t_bike
    WHERE t_bike.error_status = 3 
        AND (t_bike.bike_type = 2 OR t_bike.bike_type = 3)
        AND t_bike.city_id = 11
    GROUP BY
    	t_bike.error_status
    '''
    df_h119 = pd.read_sql(select_h119, engine_mysql)
    h119 = df_h119['vyveden_iz_ekspluatacii'].iloc[0]

    select_df2 = '''
        SELECT
            vni_cities_for_graph.id as city_id,
            vni_cities_for_graph."name",
            -- SUM(vni_cities_for_graph.poezdok),
            -- SUM(vni_cities_for_graph.kvt),
            SUM(vni_cities_for_graph.poezdok) / NULLIF(SUM(vni_cities_for_graph.kvt), 0)::decimal(16,2) AS akb_na_park,
            CASE WHEN SUM(vni_cities_for_graph.poezdok) / NULLIF(SUM(vni_cities_for_graph.kvt), 0)::decimal(16,2) < 1 THEN 0.14
                WHEN SUM(vni_cities_for_graph.poezdok) / NULLIF(SUM(vni_cities_for_graph.kvt), 0)::decimal(16,2) >= 1 AND SUM(vni_cities_for_graph.poezdok) / NULLIF(SUM(vni_cities_for_graph.kvt), 0)::decimal(16,2) < 2 THEN 0.22
                WHEN SUM(vni_cities_for_graph.poezdok) / NULLIF(SUM(vni_cities_for_graph.kvt), 0)::decimal(16,2) >= 2 THEN 0.3 END AS akb_na_park_percent
        FROM vni_cities_for_graph
        WHERE vni_cities_for_graph."date" <= DATE(TO_CHAR(current_date - interval '1' DAY, 'YYYY-mm-dd')) 
            AND vni_cities_for_graph."date" > DATE(TO_CHAR(current_date - interval '15' DAY, 'YYYY-mm-dd'))
        GROUP BY vni_cities_for_graph.id, vni_cities_for_graph."name"
    '''
    df2 = pd.read_sql(select_df2, engine_postgresql).fillna(0)

    google_service_account_json = get_google_creds()
    with open('google_json.json', 'w') as fp:
        json.dump(json.loads(google_service_account_json, strict=False), fp)
    generated_json_file = './google_json.json'

    SERVICE_ACCOUNT_FILE = './google_json.json'
    SPREADSHEET_ID = '1BMH_HSxmK33SZvv3cIAH_SIgvm2NncSTTKI1aa7CoG8'
    RANGE_NAME = 'Плановое!A1:E13'
    service_account_file = generated_json_file
    sheets_service = get_sheets_service(SERVICE_ACCOUNT_FILE)
    df3 = read_sheet_data_to_pandas(sheets_service, SPREADSHEET_ID, RANGE_NAME)

    df3['city_id'] = df3['city_id'].astype(int)
    df3['planovoye'] = df3['planovoye'].astype(int)
    df3['Batteries V4.6'] = df3['Batteries V4.6'].astype(int)
    df3['Batteries numbers V3 PRO'] = df3['Batteries numbers V3 PRO'].astype(int)

    df = df1.merge(df2[['city_id', 'akb_na_park_percent']], on='city_id', how='left') \
        .merge(df3[['city_id', 'planovoye', 'Batteries V4.6', 'Batteries numbers V3 PRO']], on='city_id', how='left') \
        .fillna(0)

    df.loc[df['city_id'] == 11, 'vyveden_iz_ekspluatacii'] = h119
    df['svobodnyh_akb'] = df['Batteries V4.6'] + df['Batteries numbers V3 PRO']
    df['skolko_nugno_akb'] = df['planovoye'] * df['akb_na_park_percent']
    df['skolko_dovesti_sim'] = df['planovoye'] - df['fact']
    df['skolko_dovesti_akb'] = df['skolko_nugno_akb'] - df['svobodnyh_akb']
    df = df.loc[df['city_id'].isin(df3['city_id'])]

    total_row = {'city_id': [''],
                 'name': ['Total'],
                 'uteryany': [df['uteryany'].sum()],
                 'v_ozhidanii_activacii': [df['v_ozhidanii_activacii'].sum()],
                 'slugebnyi_transport': [df['slugebnyi_transport'].sum()],
                 'remont': [df['remont'].sum()],
                 'vyveden_iz_ekspluatacii': [df['vyveden_iz_ekspluatacii'].sum()],
                 'mr_user': [df['mr_user'].sum()],
                 'kvt': [df['kvt'].sum()],
                 'mr_admin': [df['mr_admin'].sum()],
                 'fact': [df['fact'].loc[df['city_id'].isin([13, 15, 18, 16, 17, 19, 20, 21, 22, 25])].sum()],
                 'itogo_sim': [df['itogo_sim'].sum()],
                 'kvt_offline': [df['kvt_offline'].sum()],
                 'akb_na_park_percent': [0],
                 'planovoye': [df['planovoye'].sum()],
                 'Batteries V4.6': [df['Batteries V4.6'].sum()],
                 'Batteries numbers V3 PRO': [df['Batteries numbers V3 PRO'].sum()],
                 'svobodnyh_akb': [df['svobodnyh_akb'].sum()],
                 'skolko_nugno_akb': [df['skolko_nugno_akb'].sum()],
                 'skolko_dovesti_sim': [
                     df['skolko_dovesti_sim'].loc[df['city_id'].isin([13, 15, 18, 16, 17, 19, 20, 21, 22, 25])].sum()],
                 'skolko_dovesti_akb': [
                     df['skolko_dovesti_akb'].loc[df['city_id'].isin([13, 15, 18, 16, 17, 19, 20, 21, 22, 25])].sum()]
                 }
    df_total_row = pd.DataFrame.from_dict(total_row)
    df = pd.concat([df, df_total_row])
    df.loc[df['city_id'] == 12, 'fact'] = df[['v_ozhidanii_activacii',
                                              'slugebnyi_transport', 'remont', 'vyveden_iz_ekspluatacii', 'mr_user',
                                              'kvt', 'mr_admin']].loc[df['city_id'].isin([12])].sum().sum()
    df.loc[df['city_id'] == 11, 'fact'] = df[['v_ozhidanii_activacii',
                                              'slugebnyi_transport', 'remont', 'vyveden_iz_ekspluatacii', 'mr_user',
                                              'kvt', 'mr_admin']].loc[df['city_id'].isin([11])].sum().sum()
    a1 = df['uteryany'].sum() / 2
    a2 = df[['v_ozhidanii_activacii',
             'slugebnyi_transport',
             'remont',
             'vyveden_iz_ekspluatacii',
             'mr_user']].loc[df['city_id'].isin([13, 15, 18, 16, 17, 19, 20, 21, 22, 25])].sum().sum()
    a3 = df['fact'].loc[df['city_id'].isin([11, 12, ''])].sum()
    df.loc[df['name'] == 'Total', 'itogo_sim'] = a1 + a2 + a3
    df['timestamp'] = pd.Timestamp.now()
    df = df.iloc[:, [21, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 14, 15, 16, 17, 18, 13, 19, 20, 11, 12]]
    df.loc[df['city_id'] == 17, 'svobodnyh_akb'] = int(df.loc[df['city_id'] == 17, 'svobodnyh_akb'].iloc[0]) + int(
        df.loc[df['city_id'] == 12, 'svobodnyh_akb'].iloc[0])
    df.loc[df['name'] == 'Total', 'svobodnyh_akb'] = int(df.loc[df['name'] == 'Total', 'svobodnyh_akb'].iloc[0]) - int(
        df.loc[df['city_id'] == 12, 'svobodnyh_akb'].iloc[0])
    df.loc[df['city_id'] == 12, 'svobodnyh_akb'] = int(df.loc[df['city_id'] == 12, 'svobodnyh_akb'].iloc[0]) + int(
        df.loc[df['city_id'] == 12, 'v_ozhidanii_activacii'].iloc[0]) + int(df.loc[df['city_id'] == 12, 'remont'].iloc[0])
    df.loc[df['city_id'] == 11, 'svobodnyh_akb'] = int(df.loc[df['city_id'] == 11, 'svobodnyh_akb'].iloc[0]) + int(
        df.loc[df['city_id'] == 11, 'v_ozhidanii_activacii'].iloc[0]) + int(df.loc[df['city_id'] == 11, 'remont'].iloc[0])
    df.loc[df['city_id'] == 11, 'itogo_sim'] = df[['uteryany', 'v_ozhidanii_activacii',
                                                   'slugebnyi_transport', 'remont', 'vyveden_iz_ekspluatacii',
                                                   'mr_user',
                                                   'kvt', 'mr_admin']].loc[df['city_id'].isin([11])].sum().sum()
    df['itogo_sim_online_offline'] = df['itogo_sim'] + df['kvt_offline']
    df['planovoye'] = df['planovoye'].astype(int)
    df['planovoye'] = df['planovoye'].astype(int)
    df.loc[df['name'] == 'Total', 'city_id'] = 0
    df.to_sql("akb", engine_postgresql, if_exists="append", index=False)
    print('AKB OK!')

    # АКБ - конец





if __name__ == "__main__":
    main()
