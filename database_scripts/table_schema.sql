
-- Create Sector_Information table
CREATE TABLE Sector_Information (
    id INT IDENTITY(1,1) PRIMARY KEY,
    sector_code VARCHAR(50) NOT NULL,
    sector_name VARCHAR(100) NOT NULL,
    isActive BIT NOT NULL,
    last_updated DATETIME NOT NULL
);

-- Create Symbol_Share table
CREATE TABLE Symbol_Share (
    id INT IDENTITY(1,1) PRIMARY KEY,
    company VARCHAR(100) NOT NULL,
    total_share DECIMAL(18,2),
    Sponsor DECIMAL(18,2),
    Govt DECIMAL(18,2),
    Institute DECIMAL(18,2),
    Foreign_share DECIMAL(18,2),
    public_share DECIMAL(18,2),
    scraping_date DATETIME NOT NULL
);

-- Create Sector_Symbol table
CREATE TABLE Sector_Symbol (
    id INT IDENTITY(1,1) PRIMARY KEY,
    sector_code VARCHAR(50) NOT NULL,
    company VARCHAR(100) NOT NULL,
    last_updated DATETIME NOT NULL
);



CREATE TABLE Company_Information (
   id INT IDENTITY(1,1) PRIMARY KEY,
   company_symbol VARCHAR(20) NOT NULL,
   company_name VARCHAR(100) NOT NULL,
   isActive BIT NOT NULL DEFAULT 1,
   last_updated DATETIME NOT NULL DEFAULT GETDATE(),
);

 CREATE TABLE Symbol_LTP (
    id INT IDENTITY(1,1) PRIMARY KEY,
    company VARCHAR(255) NOT NULL,
    LTP DECIMAL(18,2) NOT NULL,
    ltp_dt DATETIME NOT NULL
);

CREATE TABLE Symbol_LTP_Hist (
    id INT IDENTITY(1,1) PRIMARY KEY,
    company VARCHAR(255) NOT NULL,
    LTP DECIMAL(18,2) NOT NULL,
    ltp_dt DATETIME NOT NULL
);




CREATE TABLE previous_history (
    id INT IDENTITY(1,1) PRIMARY KEY,
    company VARCHAR(255) NOT NULL,
	sector_code VARCHAR(255),
	total_share DECIMAL(18,2),
    LTP DECIMAL(18,2),
	ff DECIMAL(18,2),
	ff_mcap DECIMAL(18,4),
	total_mcap DECIMAL(18,4),
	pre_index DECIMAL(18,4) ,
	remark VARCHAR(255) ,
    dt DATETIME NOT NULL
);


CREATE TABLE daily_index (
    id INT PRIMARY KEY IDENTITY(1,1), -- Auto-increment ID (for SQL Server)
    sector_code INT NOT NULL,
    sector_name VARCHAR(100) NOT NULL,
    date DATE NOT NULL,
    start_index_value FLOAT NOT NULL,
    end_index_value FLOAT NOT NULL,
    daily_return FLOAT
);

CREATE TABLE previous_market_cap_data (
    id INT IDENTITY(1,1) PRIMARY KEY,
    company VARCHAR(100) NOT NULL,
    ltp FLOAT,
    timestamp DATETIME,
    total_shares BIGINT,
    market_cap FLOAT,
    free_float_pct FLOAT ,
    free_float_mcap FLOAT 
);
