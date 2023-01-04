drop database superset;
create database superset;
use superset;
CREATE TABLE mf_data (
  id int,
  name varchar(255),
  link varchar(255),
  rating INT,
  asset_value INT,
  category_id INT,
  expense_ratio decimal(10,2),
  fund_house varchar(255),
  risk varchar(100),
  risk_grade varchar(100),
  return_grade varchar(100),
  top_holdings TEXT,
  time_added DATE not null ,
  time_modified DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  primary KEY (id,time_added)
);

CREATE TABLE mf_growth (
  mf_id int not null,
  3m DECIMAL(6,2),
  6m DECIMAL(6,2),
  1y DECIMAL(6,2),
  3y DECIMAL(6,2),
  5y DECIMAL(6,2),
  7y DECIMAL(6,2),
  10y DECIMAL(6,2),
  time_added DATE not null ,
  time_modified DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  primary KEY (mf_id,time_added),
  FOREIGN KEY (mf_id,time_added) REFERENCES mf_data(id,time_added)
);

CREATE TABLE mf_credit_rating (
  mf_id int,
  aaa  DECIMAL(6,2),
  a1plus DECIMAL(6,2),
  sov DECIMAL(6,2),
  cash_equivalent DECIMAL(6,2),
  aa DECIMAL(6,2),
  a_and_below DECIMAL(6,2),
  unrated_and_others DECIMAL(6,2),
  time_added DATE not null ,
  time_modified DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  primary KEY (mf_id,time_added),
  FOREIGN KEY (mf_id,time_added) REFERENCES mf_data(id,time_added)
);

CREATE TABLE mf_risk (
  mf_id int,
  mean DECIMAL(6,2),
  std_dev DECIMAL(6,2),
  sharpe DECIMAL(6,2),
  sortino DECIMAL(6,2),
  beta DECIMAL(6,2),
  alpha DECIMAL(6,2),
  time_added DATE not null ,
  time_modified DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  primary KEY (mf_id,time_added),
  FOREIGN KEY (mf_id,time_added) REFERENCES mf_data(id,time_added)
);


CREATE TABLE mf_category_data (
  id int,
  category varchar(100),
  category_code varchar(100),
  time_added DATE not null ,
  time_modified DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id)
);

CREATE TABLE mf_category_growth (
  mf_cat_id int,
  3m DECIMAL(6,2),
  6m DECIMAL(6,2),
  1y DECIMAL(6,2),
  3y DECIMAL(6,2),
  5y DECIMAL(6,2),
  7y DECIMAL(6,2),
  10y DECIMAL(6,2),
  time_added DATE not null ,
  time_modified DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  primary KEY (mf_cat_id,time_added),
  FOREIGN KEY (mf_cat_id) REFERENCES mf_category_data(id)
);

CREATE TABLE mf_category_risk (
  mf_cat_id int,
  mean DECIMAL(6,2),
  std_dev DECIMAL(6,2),
  sharpe DECIMAL(6,2),
  sortino DECIMAL(6,2),
  beta DECIMAL(6,2),
  alpha DECIMAL(6,2),
  time_added DATE not null ,
  time_modified DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  primary KEY (mf_cat_id,time_added),
  FOREIGN KEY (mf_cat_id) REFERENCES mf_category_data(id)
);

CREATE TABLE mf_recommendation (
  id int,
  name VARCHAR(255),
  rating INT,
  total_rank INT ,
  time_added DATE not null ,
  time_modified DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  primary KEY (mf_id,time_added),
  FOREIGN KEY (mf_id) REFERENCES mf_data(id)
);