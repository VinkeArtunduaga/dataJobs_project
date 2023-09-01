import psycopg2
import csv
import json

#Carga de la configuracion de los datos de la database
with open('db_config.json') as config_file:
    config = json.load(config_file)

try:
    conn = psycopg2.connect(
        host='localhost',
        user = config['user'],
        password = config['password'],
        database='ETL'
    )

    print("Conexion exitosa uwu")

    cursor = conn.cursor()

    #Creacion de la tabla dataJobs_glassdoor
    create_table = """
        CREATE TABLE IF NOT EXISTS dataJobs_glassdoor (
            id SERIAL PRIMARY KEY,
            benefits_benefitRatingDecimal FLOAT,
            benefits_comments FLOAT,
            benefits_highlights FLOAT,
            benefits_numRatings INTEGER,
            benefits_employerSummary VARCHAR,
            breadCrumbs INTEGER,
            gaTrackerData_category INTEGER,
            gaTrackerData_empId INTEGER,
            gaTrackerData_empName VARCHAR,
            gaTrackerData_empSize VARCHAR,
            gaTrackerData_expired BOOLEAN,
            gaTrackerData_industry VARCHAR,
            gaTrackerData_industryId INTEGER,
            gaTrackerData_jobId_long FLOAT,
            gaTrackerData_jobId_int FLOAT,
            gaTrackerData_jobTitle VARCHAR,
            gaTrackerData_location VARCHAR,
            gaTrackerData_locationId INTEGER,
            gaTrackerData_locationType VARCHAR,
            gaTrackerData_pageRequestGuid_guid VARCHAR,
            gaTrackerData_pageRequestGuid_guidValid BOOLEAN,
            gaTrackerData_pageRequestGuid_part1 BIGINT,
            gaTrackerData_pageRequestGuid_part2 BIGINT,
            gaTrackerData_sector VARCHAR,
            gaTrackerData_sectorId INTEGER,
            gaTrackerData_profileConversionTrackingParams_trackingCAT VARCHAR,
            gaTrackerData_profileConversionTrackingParams_trackingSRC FLOAT,
            gaTrackerData_profileConversionTrackingParams_trackingXSP FLOAT,
            gaTrackerData_jobViewTrackingResult_jobViewDisplayTimeMillis FLOAT,
            gaTrackerData_jobViewTrackingResult_requiresTracking VARCHAR,
            gaTrackerData_jobViewTrackingResult_trackingUrl VARCHAR,
            header_adOrderId INTEGER,
            header_advertiserType VARCHAR,
            header_applicationId INTEGER,
            header_applyButtonDisabled BOOLEAN,
            header_applyUrl VARCHAR,
            header_blur BOOLEAN,
            header_coverPhoto VARCHAR,
            header_easyApply BOOLEAN,
            header_employerId INTEGER,
            header_employerName VARCHAR,
            header_expired BOOLEAN,
            header_gocId INTEGER,
            header_hideCEOInfo BOOLEAN,
            header_jobTitle VARCHAR,
            header_locId INTEGER,
            header_location VARCHAR,
            header_locationType VARCHAR,
            header_logo VARCHAR,
            header_logo2x VARCHAR,
            header_organic BOOLEAN,
            header_overviewUrl VARCHAR,
            header_posted VARCHAR,
            header_rating FLOAT,
            header_saved BOOLEAN,
            header_savedJobId INTEGER,
            header_sgocId INTEGER,
            header_sponsored BOOLEAN,
            header_userAdmin BOOLEAN,
            header_uxApplyType VARCHAR,
            header_featuredVideo VARCHAR,
            header_normalizedJobTitle VARCHAR,
            header_urgencyLabel VARCHAR,
            header_urgencyLabelForMessage VARCHAR,
            header_urgencyMessage VARCHAR,
            header_needsCommission VARCHAR,
            header_payHigh FLOAT,
            header_payLow FLOAT,
            header_payMed FLOAT,
            header_payPeriod VARCHAR,
            header_salaryHigh FLOAT,
            header_salaryLow FLOAT,
            header_salarySource VARCHAR,
            job_description VARCHAR,
            job_discoverDate VARCHAR,
            job_eolHashCode INTEGER,
            job_importConfigId INTEGER,
            job_jobReqId_long FLOAT,
            job_jobReqId_int FLOAT,
            job_jobSource VARCHAR,
            job_jobTitleId INTEGER,
            job_listingId_long FLOAT,
            job_listingId_int FLOAT,
            map_country VARCHAR,
            map_employerName VARCHAR,
            map_lat FLOAT,
            map_lng FLOAT,
            map_location VARCHAR,
            map_address VARCHAR,
            map_postalCode VARCHAR,
            overview_allBenefitsLink VARCHAR,
            overview_allPhotosLink VARCHAR,
            overview_allReviewsLink VARCHAR,
            overview_allSalariesLink VARCHAR,
            overview_foundedYear INTEGER,
            overview_hq VARCHAR,
            overview_industry VARCHAR,
            overview_industryId INTEGER,
            overview_revenue VARCHAR,
            overview_sector VARCHAR,
            overview_sectorId INTEGER,
            overview_size VARCHAR,
            overview_stock VARCHAR,
            overview_type VARCHAR,
            overview_description VARCHAR,
            overview_mission VARCHAR,
            overview_website VARCHAR,
            overview_allVideosLink VARCHAR,
            overview_competitors FLOAT,
            overview_companyVideo VARCHAR,
            photos FLOAT,
            rating_ceo_name VARCHAR,
            rating_ceo_photo VARCHAR,
            rating_ceo_photo2x VARCHAR,
            rating_ceo_ratingsCount FLOAT,
            rating_ceoApproval FLOAT,
            rating_recommendToFriend FLOAT,
            rating_starRating FLOAT,
            reviews INTEGER,
            salary_country_cc3LetterISO VARCHAR,
            salary_country_ccISO VARCHAR,
            salary_country_continent_continentCode VARCHAR,
            salary_country_continent_continentName VARCHAR,
            salary_country_continent_id FLOAT,
            salary_country_continent_new VARCHAR,
            salary_country_countryFIPS VARCHAR,
            salary_country_currency_currencyCode VARCHAR,
            salary_country_currency_defaultFractionDigits FLOAT,
            salary_country_currency_displayName VARCHAR,
            salary_country_currency_id FLOAT,
            salary_country_currency_name VARCHAR,
            salary_country_currency_negativeTemplate VARCHAR,
            salary_country_currency_new VARCHAR,
            salary_country_currency_positiveTemplate VARCHAR,
            salary_country_currency_symbol VARCHAR,
            salary_country_currencyCode VARCHAR,
            salary_country_defaultLocale VARCHAR,
            salary_country_defaultName VARCHAR,
            salary_country_defaultShortName VARCHAR,
            salary_country_employerSolutionsCountry VARCHAR,
            salary_country_id FLOAT,
            salary_country_longName VARCHAR,
            salary_country_major VARCHAR,
            salary_country_name VARCHAR,
            salary_country_new VARCHAR,
            salary_country_population FLOAT,
            salary_country_shortName VARCHAR,
            salary_country_tld VARCHAR,
            salary_country_type VARCHAR,
            salary_country_uniqueName VARCHAR,
            salary_country_usaCentricDisplayName VARCHAR,
            salary_currency_currencyCode VARCHAR,
            salary_currency_defaultFractionDigits FLOAT,
            salary_currency_displayName VARCHAR,
            salary_currency_id FLOAT,
            salary_currency_name VARCHAR,
            salary_currency_negativeTemplate VARCHAR,
            salary_currency_new VARCHAR,
            salary_currency_positiveTemplate VARCHAR,
            salary_currency_symbol VARCHAR,
            salary_lastSalaryDate VARCHAR,
            salary_salaries FLOAT,
            wwfu FLOAT
        )
    """
    cursor.execute(create_table)
    conn.commit()
    print("Tabla creada")

    # Insercion de las columnas a postgresql
    copy_query = """
        COPY dataJobs_glassdoor(
            benefits_benefitRatingDecimal, benefits_comments, benefits_highlights, 
            benefits_numRatings, benefits_employerSummary, breadCrumbs, gaTrackerData_category,
            gaTrackerData_empId, gaTrackerData_empName, gaTrackerData_empSize, gaTrackerData_expired,
            gaTrackerData_industry, gaTrackerData_industryId, gaTrackerData_jobId_long,
            gaTrackerData_jobId_int, gaTrackerData_jobTitle, gaTrackerData_location,
            gaTrackerData_locationId, gaTrackerData_locationType, gaTrackerData_pageRequestGuid_guid,
            gaTrackerData_pageRequestGuid_guidValid, gaTrackerData_pageRequestGuid_part1,
            gaTrackerData_pageRequestGuid_part2, gaTrackerData_sector, gaTrackerData_sectorId,
            gaTrackerData_profileConversionTrackingParams_trackingCAT,
            gaTrackerData_profileConversionTrackingParams_trackingSRC,
            gaTrackerData_profileConversionTrackingParams_trackingXSP,
            gaTrackerData_jobViewTrackingResult_jobViewDisplayTimeMillis,
            gaTrackerData_jobViewTrackingResult_requiresTracking,
            gaTrackerData_jobViewTrackingResult_trackingUrl, header_adOrderId,
            header_advertiserType, header_applicationId, header_applyButtonDisabled,
            header_applyUrl, header_blur, header_coverPhoto, header_easyApply,
            header_employerId, header_employerName, header_expired, header_gocId,
            header_hideCEOInfo, header_jobTitle, header_locId, header_location,
            header_locationType, header_logo, header_logo2x, header_organic,
            header_overviewUrl, header_posted, header_rating, header_saved,
            header_savedJobId, header_sgocId, header_sponsored, header_userAdmin,
            header_uxApplyType, header_featuredVideo, header_normalizedJobTitle,
            header_urgencyLabel, header_urgencyLabelForMessage, header_urgencyMessage,
            header_needsCommission, header_payHigh, header_payLow, header_payMed,
            header_payPeriod, header_salaryHigh, header_salaryLow, header_salarySource,
            job_description, job_discoverDate, job_eolHashCode, job_importConfigId,
            job_jobReqId_long, job_jobReqId_int, job_jobSource, job_jobTitleId,
            job_listingId_long, job_listingId_int, map_country, map_employerName,
            map_lat, map_lng, map_location, map_address, map_postalCode,
            overview_allBenefitsLink, overview_allPhotosLink, overview_allReviewsLink,
            overview_allSalariesLink, overview_foundedYear, overview_hq, overview_industry,
            overview_industryId, overview_revenue, overview_sector, overview_sectorId,
            overview_size, overview_stock, overview_type, overview_description,
            overview_mission, overview_website, overview_allVideosLink, overview_competitors,
            overview_companyVideo, photos, rating_ceo_name, rating_ceo_photo,
            rating_ceo_photo2x, rating_ceo_ratingsCount, rating_ceoApproval,
            rating_recommendToFriend, rating_starRating, reviews, salary_country_cc3LetterISO,
            salary_country_ccISO, salary_country_continent_continentCode,
            salary_country_continent_continentName, salary_country_continent_id,
            salary_country_continent_new, salary_country_countryFIPS,
            salary_country_currency_currencyCode, salary_country_currency_defaultFractionDigits,
            salary_country_currency_displayName, salary_country_currency_id,
            salary_country_currency_name, salary_country_currency_negativeTemplate,
            salary_country_currency_new, salary_country_currency_positiveTemplate,
            salary_country_currency_symbol, salary_country_currencyCode,
            salary_country_defaultLocale, salary_country_defaultName,
            salary_country_defaultShortName, salary_country_employerSolutionsCountry,
            salary_country_id, salary_country_longName, salary_country_major,
            salary_country_name, salary_country_new, salary_country_population,
            salary_country_shortName, salary_country_tld, salary_country_type,
            salary_country_uniqueName, salary_country_usaCentricDisplayName,
            salary_currency_currencyCode, salary_currency_defaultFractionDigits,
            salary_currency_displayName, salary_currency_id, salary_currency_name,
            salary_currency_negativeTemplate, salary_currency_new,
            salary_currency_positiveTemplate, salary_currency_symbol,
            salary_lastSalaryDate, salary_salaries, wwfu
        )
        FROM 'C:/Users/kevin/ETL/Postgres/kaggle/dataJobs_project/dataJobs_all.csv' DELIMITER ',' CSV HEADER
    """
    cursor.execute(copy_query)
    conn.commit()
    print("Datos agregados")

    #Columnas previamente revisadas que considere eliminar
    columnas_eliminadas = [
        'breadCrumbs', 'gaTrackerData_category', 'gaTrackerData_empId',
        'gaTrackerData_empSize', 'gaTrackerData_expired', 'gaTrackerData_industryId',
        'gaTrackerData_jobId_long', 'gaTrackerData_jobId_int', 'gaTrackerData_locationId',
        'gaTrackerData_locationType', 'gaTrackerData_pageRequestGuid_guid',
        'gaTrackerData_pageRequestGuid_guidValid', 'gaTrackerData_pageRequestGuid_part1',
        'gaTrackerData_pageRequestGuid_part2', 'gaTrackerData_sectorId',
        'gaTrackerData_profileConversionTrackingParams_trackingCAT',
        'gaTrackerData_profileConversionTrackingParams_trackingSRC',
        'gaTrackerData_profileConversionTrackingParams_trackingXSP',
        'gaTrackerData_jobViewTrackingResult_jobViewDisplayTimeMillis',
        'gaTrackerData_jobViewTrackingResult_requiresTracking',
        'gaTrackerData_jobViewTrackingResult_trackingUrl', 'header_adOrderId',
        'header_advertiserType', 'header_applicationId', 'header_applyButtonDisabled',
        'header_applyUrl', 'header_blur', 'header_coverPhoto', 'header_easyApply',
        'header_employerId', 'header_expired', 'header_gocId', 'header_hideCEOInfo',
        'header_locId', 'header_locationType', 'header_logo', 'header_logo2x',
        'header_organic', 'header_overviewUrl', 'header_rating', 'header_saved',
        'header_savedJobId', 'header_sgocId', 'header_sponsored', 'header_userAdmin',
        'header_uxApplyType', 'header_featuredVideo', 'header_urgencyLabel',
        'header_urgencyLabelForMessage', 'header_urgencyMessage', 'header_needsCommission',
        'header_payHigh', 'header_payLow', 'header_payMed', 'header_payPeriod',
        'header_salaryHigh', 'header_salaryLow', 'header_salarySource', 'job_eolHashCode',
        'job_importConfigId', 'job_jobReqId_long', 'job_jobReqId_int', 'job_jobTitleId',
        'job_listingId_long', 'job_listingId_int', 'map_address', 'map_postalCode',
        'overview_allBenefitsLink', 'overview_allPhotosLink', 'overview_allReviewsLink',
        'overview_allSalariesLink', 'overview_industryId', 'overview_sectorId',
        'overview_stock', 'overview_mission', 'overview_allVideosLink',
        'overview_competitors', 'overview_companyVideo', 'photos', 'rating_ceo_photo',
        'rating_ceo_photo2x', 'rating_ceo_ratingsCount', 'rating_ceoApproval',
        'rating_recommendToFriend', 'salary_country_cc3LetterISO', 'salary_country_ccISO',
        'salary_country_continent_continentCode', 'salary_country_continent_continentName',
        'salary_country_continent_id', 'salary_country_continent_new', 'salary_country_countryFIPS',
        'salary_country_currency_currencyCode', 'salary_country_currency_defaultFractionDigits',
        'salary_country_currency_displayName', 'salary_country_currency_id',
        'salary_country_currency_name', 'salary_country_currency_negativeTemplate',
        'salary_country_currency_new', 'salary_country_currency_positiveTemplate',
        'salary_country_currency_symbol', 'salary_country_currencyCode',
        'salary_country_defaultLocale', 'salary_country_defaultShortName',
        'salary_country_employerSolutionsCountry', 'salary_country_id',
        'salary_country_longName', 'salary_country_major', 'salary_country_name',
        'salary_country_new', 'salary_country_population', 'salary_country_shortName',
        'salary_country_tld', 'salary_country_type', 'salary_country_uniqueName',
        'salary_country_usaCentricDisplayName', 'salary_currency_currencyCode',
        'salary_currency_defaultFractionDigits', 'salary_currency_displayName',
        'salary_currency_id', 'salary_currency_name', 'salary_currency_negativeTemplate',
        'salary_currency_new', 'salary_currency_positiveTemplate', 'salary_currency_symbol',
        'salary_lastSalaryDate', 'wwfu'
    ]

    # Recorro columna por columna para eliminarlas
    for columna in columnas_eliminadas:
        drop_column_query = f"ALTER TABLE dataJobs_glassdoor DROP COLUMN IF EXISTS {columna};"
        cursor.execute(drop_column_query)
        print(f"Columna {columna} eliminada")
    conn.commit()

    # Reemplazar valores nulos en las columnas mencionadas
    update_query = """
        UPDATE dataJobs_glassdoor
        SET
            benefits_employerSummary = COALESCE(benefits_employerSummary, 'Without a summary of benefits'),
            gaTrackerData_location = COALESCE(gaTrackerData_location, 'NA'),
            header_normalizedJobTitle = COALESCE(header_normalizedJobTitle, 'Not normalized'),
            overview_description = COALESCE(overview_description, 'Without description'),
            overview_website = COALESCE(overview_website, 'No website'),
            rating_ceo_name = COALESCE(rating_ceo_name, 'No ceo name')
    """
    cursor.execute(update_query)
    conn.commit()
    print("Valores nulos reemplazados")

    # Consulta para obtener valores únicos de la columna header_jobTitle
    unique_job_titles_query = """
        SELECT DISTINCT header_jobTitle
        FROM dataJobs_glassdoor
    """
    cursor.execute(unique_job_titles_query)
    unique_job_titles = cursor.fetchall()
    job_titles_list = [title[0] for title in unique_job_titles]
    job_titles_str = ', '.join(job_titles_list)
    print("Valores únicos de header_jobTitle:")
    print(job_titles_str)

    # Creacion de la columna jobTitle_normalized
    Job_normalized_column = """
        ALTER TABLE IF EXISTS dataJobs_glassdoor
        ADD COLUMN IF NOT EXISTS jobTitle_normalized VARCHAR;
    """
    cursor.execute(Job_normalized_column)
    conn.commit()
    print("Columna jobTitle_normalized agregada")

    # Consulta para obtener los valores únicos de la columna 'jobTitle_normalized'
    query_jobTitle = "SELECT DISTINCT jobTitle_normalized FROM dataJobs_glassdoor"
    cursor.execute(query_jobTitle)

    # Obtener los resultados y imprimir los valores únicos
    unique_titles = cursor.fetchall()
    for title in unique_titles:
        print(title[0])  

    # Actualizar la columna jobTitle_normalized basada en los valores de header_jobTitle
    update_jobtitle = """
        UPDATE dataJobs_glassdoor
        SET jobTitle_normalized = CASE
            WHEN header_jobTitle ILIKE '%Business Analyst%' THEN 'Business'
            WHEN header_jobTitle ILIKE '%Business%' THEN 'Business'
            WHEN header_jobTitle ILIKE '%Product Owner%' THEN 'Business' 
            WHEN header_jobTitle ILIKE '%Schadebeheerder%' THEN 'Business' 
            WHEN header_jobTitle ILIKE '%Marketing Specialist%' THEN 'Business' 
            WHEN header_jobTitle ILIKE '%Product%' THEN 'Business' 
            WHEN header_jobTitle ILIKE '%Project Buyer%' THEN 'Business' 
            WHEN header_jobTitle ILIKE '%Accountancy%' THEN 'Business' 
            WHEN header_jobTitle ILIKE '%Accountant%' THEN 'Business' 
            WHEN header_jobTitle ILIKE '%DOSSIERBEHEERDER%' THEN 'Business' 
            WHEN header_jobTitle ILIKE '%Dossierbeheerder%' THEN 'Business' 
            WHEN header_jobTitle ILIKE '%Contractbeheerder%' THEN 'Business'
            WHEN header_jobTitle ILIKE '%Løsningsarkitekter%' THEN 'Business' 
            WHEN header_jobTitle ILIKE '%Klantenbeheerder%' THEN 'Business' 
            WHEN header_jobTitle ILIKE '%Operationeel Beheerder%' THEN 'Business' 
            WHEN header_jobTitle ILIKE '%Pöyry%' THEN 'Business' 
            WHEN header_jobTitle ILIKE '%Project Administration%' THEN 'Business' 
            WHEN header_jobTitle ILIKE '%Relatiebeheerder%' THEN 'Business'
            WHEN header_jobTitle ILIKE '%Materials Science%' THEN 'Healthcare/Science'
            WHEN header_jobTitle ILIKE '%Clinical Research%' THEN 'Healthcare/Science' 
            WHEN header_jobTitle ILIKE '%Scientist%' THEN 'Healthcare/Science'
            WHEN header_jobTitle ILIKE '%Computational Biologist%' THEN 'Healthcare/Science' 
            WHEN header_jobTitle ILIKE '%Medical%' THEN 'Healthcare/Science'
            WHEN header_jobTitle ILIKE '%Sr Lab%' THEN 'Healthcare/Science' 
            WHEN header_jobTitle ILIKE '%Molecular%' THEN 'Healthcare/Science'
            WHEN header_jobTitle ILIKE '%Global HR%' THEN 'Human Resources' 
            WHEN header_jobTitle ILIKE '%Payrollbeheerder%' THEN 'Human Resources'
            WHEN header_jobTitle ILIKE '%Analyst%' THEN 'Data Science/Analytics' 
            WHEN header_jobTitle ILIKE '%Database Administrator%' THEN 'Data Science/Analytics'  
            WHEN header_jobTitle ILIKE '%Datenbank%' THEN 'Data Science/Analytics'
            WHEN header_jobTitle ILIKE '%Databasebeheerder%' THEN 'Data Science/Analytics' 
            WHEN header_jobTitle ILIKE '%Deep Learning%' THEN 'Data Science/Analytics' 
            WHEN header_jobTitle ILIKE '%Deep-learning%' THEN 'Data Science/Analytics' 
            WHEN header_jobTitle ILIKE '%Data%' THEN 'Data Science/Analytics'
            WHEN header_jobTitle ILIKE '%Server DBA%' THEN 'Data Science/Analytics' 
            WHEN header_jobTitle ILIKE '%Données%' THEN 'Data Science/Analytics'
            WHEN header_jobTitle ILIKE '%DBA%' THEN 'Data Science/Analytics'
            WHEN header_jobTitle ILIKE '%INGÉNIEUR%' THEN 'Engineering' 
            WHEN header_jobTitle ILIKE '%Eng%' THEN 'Engineering'
            WHEN header_jobTitle ILIKE '%Engr%' THEN 'Engineering' 
            WHEN header_jobTitle ILIKE '%ingenieur%' THEN 'Engineering' 
            WHEN header_jobTitle ILIKE '%Projectbeheerder%' THEN 'Engineering' 
            WHEN header_jobTitle ILIKE '%Wagenparkbeheerder%' THEN 'Engineering'
            WHEN header_jobTitle ILIKE '%Project Administrator%' THEN 'Management'
            WHEN header_jobTitle ILIKE '%Projects Financial%' THEN 'Management' 
            WHEN header_jobTitle ILIKE '%Project Coordinator%' THEN 'Management'
            WHEN header_jobTitle ILIKE '%Project Management%' THEN 'Management' 
            WHEN header_jobTitle ILIKE '%Project Planning%' THEN 'Management'
            WHEN header_jobTitle ILIKE '%Functioneel Beheerder%' THEN 'Management' 
            WHEN header_jobTitle ILIKE '%Program Coordinator%' THEN 'Management' 
            WHEN header_jobTitle ILIKE '%Project Specialist%' THEN 'Management' 
            WHEN header_jobTitle ILIKE '%Project Assistant%' THEN 'Management'
            WHEN header_jobTitle ILIKE '%Project Support%' THEN 'Management' 
            WHEN header_jobTitle ILIKE '%Delivery Project%' THEN 'Management' 
            WHEN header_jobTitle ILIKE '%Program Management%' THEN 'Management' 
            WHEN header_jobTitle ILIKE '%Project Co-ordinator%' THEN 'Management' 
            WHEN header_jobTitle ILIKE '%Project Officer%' THEN 'Management'
            WHEN header_jobTitle ILIKE '%MANAGEMENT%' THEN 'Management' 
            WHEN header_jobTitle ILIKE '%Project Planner%' THEN 'Management'
            WHEN header_jobTitle ILIKE '%Project Controller%' THEN 'Management' 
            WHEN header_jobTitle ILIKE '%Project Office%' THEN 'Management' 
            WHEN header_jobTitle ILIKE '%Project Admin%' THEN 'Management' 
            WHEN header_jobTitle ILIKE '%Projectvoorbereider%' THEN 'Management' 
            WHEN header_jobTitle ILIKE '%Projectleider%' THEN 'Management'
            WHEN header_jobTitle ILIKE '%Applicatiebeheerder%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%Systeembeheerder%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%Programmer%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%SOFTWARE%' THEN 'Technology/IT'
            WHEN header_jobTitle ILIKE '%Computer Scientist%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%Software Developer%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%Machine Learning%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%Werkplekbeheerder%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%Netwerkbeheerder%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%MJUKVARUUTVECKLARE%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%Technology Consulting%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%Technischer Berater%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%Beheerder IT%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%Développement Java%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%Cloud applicatie%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%ICT beheerder%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%Linux Beheerder%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%Project Executive%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%Angular%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%Informatie analist%' THEN 'Technology/IT'
            WHEN header_jobTitle ILIKE '%IT%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%Technisch Beheerder%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%Technisch%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%Technieker%' THEN 'Technology/IT'
            WHEN header_jobTitle ILIKE '%Burgerzaken%' THEN 'Government' 
            WHEN header_jobTitle ILIKE '%Ketenbeheerder%' THEN 'Logistics/Supply Chain' 
            WHEN header_jobTitle ILIKE '%stockbeheerder%' THEN 'Logistics/Supply Chain' 
            WHEN header_jobTitle ILIKE '%Magazijnbeheerder%' THEN 'Logistics/Supply Chain' 
            WHEN header_jobTitle ILIKE '%Orderbeheerder%' THEN 'Logistics/Supply Chain' 
            WHEN header_jobTitle ILIKE '%Magazijn%' THEN 'Logistics/Supply Chain'  
            WHEN header_jobTitle ILIKE '%Project Material%' THEN 'Logistics/Supply Chain'
            WHEN header_jobTitle ILIKE '%Research%' THEN 'Research' 
            WHEN header_jobTitle ILIKE '%Building%' THEN 'Architecture/Construction' 
            WHEN header_jobTitle ILIKE '%BIM Modeller%' THEN 'Architecture/Construction' 
            WHEN header_jobTitle ILIKE '%Modelleur%' THEN 'Architecture/Construction' 
            WHEN header_jobTitle ILIKE '%Construction%' THEN 'Architecture/Construction'
            WHEN header_jobTitle ILIKE '%到校學前康復服務%' THEN 'Education' 
            WHEN header_jobTitle ILIKE '%School%' THEN 'Education' 
            WHEN header_jobTitle ILIKE '%Cateringbeheerder%' THEN 'Food Industry' 
            WHEN header_jobTitle ILIKE '%Manufacturing%' THEN 'Manufacturing'  
            WHEN header_jobTitle ILIKE '%Debiteurenbeheerder%' THEN 'Manufacturing'
            WHEN header_jobTitle ILIKE '%Transportation%' THEN 'Transportation' 
            WHEN header_jobTitle ILIKE '%Fleet Programs%' THEN 'Transportation' 
            WHEN header_jobTitle ILIKE '%Polisbeheerder%' THEN 'Transportation' 
            WHEN header_jobTitle ILIKE '%Hotelbeheerder%' THEN 'Tourism/Hospitality'  
            WHEN header_jobTitle ILIKE '%Commercieel bediende%' THEN 'Retail' 
            WHEN header_jobTitle ILIKE '%Modelmaker%' THEN 'Design'
            ELSE jobTitle_normalized
        END;
    """
    cursor.execute(update_jobtitle)
    conn.commit()
    print("Valores actualizados en jobTitle_normalized")

    # Cambio de vacios en jobTilte_normalized por otros
    update_empty = """
        UPDATE dataJobs_glassdoor
        SET jobTitle_normalized = 'Other'
        WHERE jobTitle_normalized = '';
    """
    cursor.execute(update_empty)
    conn.commit()
    print("Campos vacios cambiados")

    # Cambio de nulos en overview type
    update_null_overview_type= """
        UPDATE dataJobs_glassdoor
        SET overview_type = 'Unknown'
        WHERE overview_type IS NULL;
    """
    cursor.execute(update_null_overview_type)
    conn.commit()
    print("Valores nulos en overview_type actualizados")

except Exception as ex:
    print("Error:", ex)

finally:
    if conn:
        cursor.close()
        conn.close()
        print("Conexión finalizada u.u")
