import tushare as ts
from datetime import datetime
import pandas as pd
import numpy as np
import json

# 设置显示的最大行数
pd.set_option('display.max_rows', None)
# 设置显示的最大列数
pd.set_option('display.max_columns', None)
# 设置宽度选项以适应所有列
pd.set_option('display.width', None)
# 设置行宽选项以避免换行
pd.set_option('display.expand_frame_repr', False)

def get_last_trade_date(pro, date):
    # date格式是YYYYMMDD格式的字符串
    df = pro.trade_cal(cal_date=date, fields=["pretrade_date"])
    return df['pretrade_date'].iloc[-1]


def valuation_bank(df, col_name, window_size):
    # 选择所需的列
    df = df[['ts_code', 'trade_date', col_name]].copy()  # 使用.copy()来确保我们有一个副本
    df = df[::-1].reset_index(drop=True) # 反向排序df
    # 使用rolling窗口计算最大值、最小值
    rolling_max = df[col_name].rolling(window=window_size, min_periods=1).max()
    rolling_min = df[col_name].rolling(window=window_size, min_periods=1).min()

    # 使用.loc[]来设置值，以避免警告
    df.loc[:, col_name + '_max'] = rolling_max
    df.loc[:, col_name + '_min'] = rolling_min
    df.loc[:, col_name + '_mean'] = (df.loc[:, col_name + '_max']+df.loc[:, col_name + '_min'])/2
    df.loc[:, col_name + '_75%'] = (df.loc[:, col_name + '_max']+df.loc[:, col_name + '_mean'])/2
    df.loc[:, col_name + '_25%'] = (df.loc[:, col_name + '_min']+df.loc[:, col_name + '_mean'])/2

    # 均值化
    df.loc[:, col_name + '_max'] = round(df.loc[:, col_name + '_max'].rolling(window=10, min_periods=1).mean(),4)
    df.loc[:, col_name + '_min'] = round(df.loc[:, col_name + '_min'].rolling(window=10, min_periods=1).mean(),4)
    df.loc[:, col_name + '_mean'] = round(df.loc[:, col_name + '_mean'].rolling(window=10, min_periods=1).mean(),4)
    df.loc[:, col_name + '_75%'] = round(df.loc[:, col_name + '_75%'].rolling(window=10, min_periods=1).mean(),4)
    df.loc[:, col_name + '_25%'] = round(df.loc[:, col_name + '_25%'].rolling(window=10, min_periods=1).mean(),4)

    return df


def get_dates(counter):
    """
    获取当前日期和 counter 天之前日期，格式为 "YYYYMMDD"。

    参数:
    - counter: int, 表示相对于当前日期向前回溯的天数。

    返回:
    - tuple, 包含两个字符串，第一个字符串是当前日期，第二个字符串是 counter 天之前日期。
    """

    # 获取当前日期
    today = pd.Timestamp.now()

    # 计算当前日期
    end_date = today

    # 计算 counter 天之前的日期
    start_date = today - pd.DateOffset(days=counter)

    # 将日期转换为 "YYYYMMDD" 格式
    end_date_str = end_date.strftime("%Y%m%d")
    start_date_str = start_date.strftime("%Y%m%d")

    return start_date_str, end_date_str

def get_quarters(counter):
    """
    获取当前季度的最后一日和 counter 个季度之前最后一日的日期，格式为 "YYYYMMDD"。

    参数:
    - counter: int, 表示相对于当前季度向前回溯的季度数。

    返回:
    - tuple, 包含两个字符串，第一个字符串是当前季度的最后一日，第二个字符串是 counter 个季度之前最后一日。
    """

    # 获取当前日期
    today = pd.Timestamp.now()

    # 计算当前季度的最后一日
    end_date = today.to_period('Q').end_time

    # 计算 counter 个季度之前的季度的最后一日
    start_date = (today - pd.DateOffset(months=3*counter)).to_period('Q').end_time

    # 将日期转换为 "YYYYMMDD" 格式
    end_date_str = end_date.strftime("%Y%m%d")
    start_date_str = start_date.strftime("%Y%m%d")

    return start_date_str, end_date_str

def get_years(counter):
    """
    获取当前年度的最后一日和 counter 个年度之前最后一日的日期，格式为 "YYYYMMDD"。

    参数:
    - counter: int, 表示相对于当前年度向前回溯的年度数。

    返回:
    - tuple, 包含两个字符串，第一个字符串是当前年度的最后一日，第二个字符串是 counter 个年度之前最后一日。
    """

    # 获取当前日期
    today = pd.Timestamp.now()

    # 计算当前年度的最后一日
    end_date = today.to_period('Y').end_time

    # 计算 counter 个年度之前的年度的最后一日
    start_date = (today - pd.DateOffset(years=counter)).to_period('Y').end_time

    # 将日期转换为 "YYYYMMDD" 格式
    end_date_str = end_date.strftime("%Y%m%d")
    start_date_str = start_date.strftime("%Y%m%d")

    return start_date_str, end_date_str

# 一、个股基础信息
def basic_info(pro, ts_code):
    mapper = {
        "ts_code": "股票代码",
        "com_name": "公司全称",
        "com_id": "统一社会信用代码",
        "exchange": "交易所代码",
        "chairman": "法人代表",
        "manager": "总经理",
        "secretary": "董秘",
        "reg_capital": "注册资本(万元)",
        "setup_date": "注册日期",
        "province": "所在省份",
        "city": "所在城市",
        "introduction": "公司介绍",
        "website": "公司主页",
        "email": "电子邮件",
        "office": "办公室",
        "employees": "员工人数",
        "main_business": "主要业务及产品",
        "business_scope": "经营范围",
    }
    # 指定字段
    fields = [
        "ts_code",
        "com_name",
        "com_id",
        "chairman",
        "manager",
        "secretary",
        "reg_capital",
        "setup_date",
        "province",
        "city",
        "website",
        "email",
        "employees",
        "exchange",
        "introduction",
        "office",
        "ann_date",
        "business_scope",
        "main_business"
    ]
    # 获取数据
    df = pro.stock_company(ts_code=ts_code, fields=fields)
    # 重名数据字段
    df = df.rename(columns=mapper)
    return df

# 二、筹码分布
def daily_chips_and_winrate(pro, ts_code, data_len):
    mapper = {
        "ts_code": "股票代码",
        "trade_date": "交易日期",
        "his_low": "历史最低价",
        "his_high": "历史最高价",
        "cost_5pct": "5分位成本",
        "cost_15pct": "15分位成本",
        "cost_50pct": "50分位成本",
        "cost_85pct": "85分位成本",
        "cost_95pct": "95分位成本",
        "weight_avg": "加权平均成本",
        "winner_rate": "胜率",
        "close": "收盘价"
    }
    # 获取开始时间与结束时间
    start_date, end_date = get_dates(data_len)
    # 获取筹码分布数据
    df_cyq_perf = pro.cyq_perf(ts_code=ts_code, start_date=start_date, end_date=end_date)
    # 获取日线行情数据
    df_daily = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date, fields=["close", "trade_date"])
    # 合并两个表
    df_cyq_perf = pd.merge(df_cyq_perf, df_daily, on='trade_date', how='left')
    # 重名数据字段
    df = df_cyq_perf.rename(columns=mapper)
    return df

# 三、财务-利润表
def fin_income(pro, ts_code, data_len):
    mapper = {
        "ts_code": "TS代码",
        "ann_date": "公告日期",
        "f_ann_date": "实际公告日期",
        "end_date": "报告期",
        "report_type": "报告类型 见底部表",
        "comp_type": "公司类型(1一般工商业2银行3保险4证券)",
        "end_type": "报告期类型",
        "basic_eps": "基本每股收益",
        "diluted_eps": "稀释每股收益",
        "total_revenue": "营业总收入",
        "revenue": "营业收入",
        "int_income": "利息收入",
        "prem_earned": "已赚保费",
        "comm_income": "手续费及佣金收入",
        "n_commis_income": "手续费及佣金净收入",
        "n_oth_income": "其他经营净收益",
        "n_oth_b_income": "加:其他业务净收益",
        "prem_income": "保险业务收入",
        "out_prem": "减:分出保费",
        "une_prem_reser": "提取未到期责任准备金",
        "reins_income": "其中:分保费收入",
        "n_sec_tb_income": "代理买卖证券业务净收入",
        "n_sec_uw_income": "证券承销业务净收入",
        "n_asset_mg_income": "受托客户资产管理业务净收入",
        "oth_b_income": "其他业务收入",
        "fv_value_chg_gain": "加:公允价值变动净收益",
        "invest_income": "加:投资净收益",
        "ass_invest_income": "其中:对联营企业和合营企业的投资收益",
        "forex_gain": "加:汇兑净收益",
        "total_cogs": "营业总成本",
        "oper_cost": "减:营业成本",
        "int_exp": "减:利息支出",
        "comm_exp": "减:手续费及佣金支出",
        "biz_tax_surchg": "减:营业税金及附加",
        "sell_exp": "减:销售费用",
        "admin_exp": "减:管理费用",
        "fin_exp": "减:财务费用",
        "assets_impair_loss": "减:资产减值损失",
        "prem_refund": "退保金",
        "compens_payout": "赔付总支出",
        "reser_insur_liab": "提取保险责任准备金",
        "div_payt": "保户红利支出",
        "reins_exp": "分保费用",
        "oper_exp": "营业支出",
        "compens_payout_refu": "减:摊回赔付支出",
        "insur_reser_refu": "减:摊回保险责任准备金",
        "reins_cost_refund": "减:摊回分保费用",
        "other_bus_cost": "其他业务成本",
        "operate_profit": "营业利润",
        "non_oper_income": "加:营业外收入",
        "non_oper_exp": "减:营业外支出",
        "nca_disploss": "其中:减:非流动资产处置净损失",
        "total_profit": "利润总额",
        "income_tax": "所得税费用",
        "n_income": "净利润(含少数股东损益)",
        "n_income_attr_p": "净利润(不含少数股东损益)",
        "minority_gain": "少数股东损益",
        "oth_compr_income": "其他综合收益",
        "t_compr_income": "综合收益总额",
        "compr_inc_attr_p": "归属于母公司(或股东)的综合收益总额",
        "compr_inc_attr_m_s": "归属于少数股东的综合收益总额",
        "ebit": "息税前利润",
        "ebitda": "息税折旧摊销前利润",
        "insurance_exp": "保险业务支出",
        "undist_profit": "年初未分配利润",
        "distable_profit": "可分配利润",
        "rd_exp": "研发费用",
        "fin_exp_int_exp": "财务费用:利息费用",
        "fin_exp_int_inc": "财务费用:利息收入",
        "transfer_surplus_rese": "盈余公积转入",
        "transfer_housing_imprest": "住房周转金转入",
        "transfer_oth": "其他转入",
        "adj_lossgain": "调整以前年度损益",
        "withdra_legal_surplus": "提取法定盈余公积",
        "withdra_legal_pubfund": "提取法定公益金",
        "withdra_biz_devfund": "提取企业发展基金",
        "withdra_rese_fund": "提取储备基金",
        "withdra_oth_ersu": "提取任意盈余公积金",
        "workers_welfare": "职工奖金福利",
        "distr_profit_shrhder": "可供股东分配的利润",
        "prfshare_payable_dvd": "应付优先股股利",
        "comshare_payable_dvd": "应付普通股股利",
        "capit_comstock_div": "转作股本的普通股股利",
        "net_after_nr_lp_correct": "扣除非经常性损益后的净利润（更正前）",
        "credit_impa_loss": "信用减值损失",
        "net_expo_hedging_benefits": "净敞口套期收益",
        "oth_impair_loss_assets": "其他资产减值损失",
        "total_opcost": "营业总成本（二）",
        "amodcost_fin_assets": "以摊余成本计量的金融资产终止确认收益",
        "oth_income": "其他收益",
        "asset_disp_income": "资产处置收益",
        "continued_net_profit": "持续经营净利润",
        "end_net_profit": "终止经营净利润",
        "update_flag": "更新标识",
    }

    # 获取开始时间与结束时间
    start_date, end_date = get_quarters(data_len)
    # 获取数据
    df = pro.income(ts_code=ts_code, start_date=start_date, end_date=end_date)
    # 若有更新，则取最新值
    df = df.sort_values(by='update_flag', ascending=False).groupby("end_date").first().reset_index()
    # 去除一整列皆为空的列
    df = df.loc[:, df.count() > 0]
    # 把Nan处理为0
    df = df.fillna(0)
    # 重名数据字段
    df = df.rename(columns=mapper)
    return df

# 四、财务-资产负债表
def fin_balancesheet(pro, ts_code, data_len):
    mapper = {
        "ts_code": "TS股票代码",
        "ann_date": "公告日期",
        "f_ann_date": "实际公告日期",
        "end_date": "报告期",
        "report_type": "报表类型",
        "comp_type": "公司类型(1一般工商业2银行3保险4证券)",
        "end_type": "报告期类型",
        "total_share": "期末总股本",
        "cap_rese": "资本公积金",
        "undistr_porfit": "未分配利润",
        "surplus_rese": "盈余公积金",
        "special_rese": "专项储备",
        "money_cap": "货币资金",
        "trad_asset": "交易性金融资产",
        "notes_receiv": "应收票据",
        "accounts_receiv": "应收账款",
        "oth_receiv": "其他应收款",
        "prepayment": "预付款项",
        "div_receiv": "应收股利",
        "int_receiv": "应收利息",
        "inventories": "存货",
        "amor_exp": "待摊费用",
        "nca_within_1y": "一年内到期的非流动资产",
        "sett_rsrv": "结算备付金",
        "loanto_oth_bank_fi": "拆出资金",
        "premium_receiv": "应收保费",
        "reinsur_receiv": "应收分保账款",
        "reinsur_res_receiv": "应收分保合同准备金",
        "pur_resale_fa": "买入返售金融资产",
        "oth_cur_assets": "其他流动资产",
        "total_cur_assets": "流动资产合计",
        "fa_avail_for_sale": "可供出售金融资产",
        "htm_invest": "持有至到期投资",
        "lt_eqt_invest": "长期股权投资",
        "invest_real_estate": "投资性房地产",
        "time_deposits": "定期存款",
        "oth_assets": "其他资产",
        "lt_rec": "长期应收款",
        "fix_assets": "固定资产",
        "cip": "在建工程",
        "const_materials": "工程物资",
        "fixed_assets_disp": "固定资产清理",
        "produc_bio_assets": "生产性生物资产",
        "oil_and_gas_assets": "油气资产",
        "intan_assets": "无形资产",
        "r_and_d": "研发支出",
        "goodwill": "商誉",
        "lt_amor_exp": "长期待摊费用",
        "defer_tax_assets": "递延所得税资产",
        "decr_in_disbur": "发放贷款及垫款",
        "oth_nca": "其他非流动资产",
        "total_nca": "非流动资产合计",
        "cash_reser_cb": "现金及存放中央银行款项",
        "depos_in_oth_bfi": "存放同业和其它金融机构款项",
        "prec_metals": "贵金属",
        "deriv_assets": "衍生金融资产",
        "rr_reins_une_prem": "应收分保未到期责任准备金",
        "rr_reins_outstd_cla": "应收分保未决赔款准备金",
        "rr_reins_lins_liab": "应收分保寿险责任准备金",
        "rr_reins_lthins_liab": "应收分保长期健康险责任准备金",
        "refund_depos": "存出保证金",
        "ph_pledge_loans": "保户质押贷款",
        "refund_cap_depos": "存出资本保证金",
        "indep_acct_assets": "独立账户资产",
        "client_depos": "其中：客户资金存款",
        "client_prov": "其中：客户备付金",
        "transac_seat_fee": "其中:交易席位费",
        "invest_as_receiv": "应收款项类投资",
        "total_assets": "资产总计",
        "lt_borr": "长期借款",
        "st_borr": "短期借款",
        "cb_borr": "向中央银行借款",
        "depos_ib_deposits": "吸收存款及同业存放",
        "loan_oth_bank": "拆入资金",
        "trading_fl": "交易性金融负债",
        "notes_payable": "应付票据",
        "acct_payable": "应付账款",
        "adv_receipts": "预收款项",
        "sold_for_repur_fa": "卖出回购金融资产款",
        "comm_payable": "应付手续费及佣金",
        "payroll_payable": "应付职工薪酬",
        "taxes_payable": "应交税费",
        "int_payable": "应付利息",
        "div_payable": "应付股利",
        "oth_payable": "其他应付款",
        "acc_exp": "预提费用",
        "deferred_inc": "递延收益",
        "st_bonds_payable": "应付短期债券",
        "payable_to_reinsurer": "应付分保账款",
        "rsrv_insur_cont": "保险合同准备金",
        "acting_trading_sec": "代理买卖证券款",
        "acting_uw_sec": "代理承销证券款",
        "non_cur_liab_due_1y": "一年内到期的非流动负债",
        "oth_cur_liab": "其他流动负债",
        "total_cur_liab": "流动负债合计",
        "bond_payable": "应付债券",
        "lt_payable": "长期应付款",
        "specific_payables": "专项应付款",
        "estimated_liab": "预计负债",
        "defer_tax_liab": "递延所得税负债",
        "defer_inc_non_cur_liab": "递延收益-非流动负债",
        "oth_ncl": "其他非流动负债",
        "total_ncl": "非流动负债合计",
        "depos_oth_bfi": "同业和其它金融机构存放款项",
        "deriv_liab": "衍生金融负债",
        "depos": "吸收存款",
        "agency_bus_liab": "代理业务负债",
        "oth_liab": "其他负债",
        "prem_receiv_adva": "预收保费",
        "depos_received": "存入保证金",
        "ph_invest": "保户储金及投资款",
        "reser_une_prem": "未到期责任准备金",
        "reser_outstd_claims": "未决赔款准备金",
        "reser_lins_liab": "寿险责任准备金",
        "reser_lthins_liab": "长期健康险责任准备金",
        "indept_acc_liab": "独立账户负债",
        "pledge_borr": "其中:质押借款",
        "indem_payable": "应付赔付款",
        "policy_div_payable": "应付保单红利",
        "total_liab": "负债合计",
        "treasury_share": "减:库存股",
        "ordin_risk_reser": "一般风险准备",
        "forex_differ": "外币报表折算差额",
        "invest_loss_unconf": "未确认的投资损失",
        "minority_int": "少数股东权益",
        "total_hldr_eqy_exc_min_int": "股东权益合计(不含少数股东权益)",
        "total_hldr_eqy_inc_min_int": "股东权益合计(含少数股东权益)",
        "total_liab_hldr_eqy": "负债及股东权益总计",
        "lt_payroll_payable": "长期应付职工薪酬",
        "oth_comp_income": "其他综合收益",
        "oth_eqt_tools": "其他权益工具",
        "oth_eqt_tools_p_shr": "其他权益工具(优先股)",
        "lending_funds": "融出资金",
        "acc_receivable": "应收款项",
        "st_fin_payable": "应付短期融资款",
        "payables": "应付款项",
        "hfs_assets": "持有待售的资产",
        "hfs_sales": "持有待售的负债",
        "cost_fin_assets": "以摊余成本计量的金融资产",
        "fair_value_fin_assets": "以公允价值计量且其变动计入其他综合收益的金融资产",
        "cip_total": "在建工程(合计)(元)",
        "oth_pay_total": "其他应付款(合计)(元)",
        "long_pay_total": "长期应付款(合计)(元)",
        "debt_invest": "债权投资(元)",
        "oth_debt_invest": "其他债权投资(元)",
        "oth_eq_invest": "其他权益工具投资(元)",
        "oth_illiq_fin_assets": "其他非流动金融资产(元)",
        "oth_eq_ppbond": "其他权益工具:永续债(元)",
        "receiv_financing": "应收款项融资",
        "use_right_assets": "使用权资产",
        "lease_liab": "租赁负债",
        "contract_assets": "合同资产",
        "contract_liab": "合同负债",
        "accounts_receiv_bill": "应收票据及应收账款",
        "accounts_pay": "应付票据及应付账款",
        "oth_rcv_total": "其他应收款(合计)（元）",
        "fix_assets_total": "固定资产(合计)(元)",
        "update_flag": "更新标识",
    }

    # 获取开始时间与结束时间
    start_date, end_date = get_quarters(data_len)
    # 获取数据
    df = pro.balancesheet(ts_code=ts_code, start_date=start_date, end_date=end_date)
    # 若有更新，则取最新值
    df = df.sort_values(by='update_flag', ascending=False).groupby("end_date").first().reset_index()
    # 去除一整列皆为空的列
    df = df.loc[:, df.count() > 0]
    # 把Nan处理为0
    df = df.fillna(0)
    # 重名数据字段
    df = df.rename(columns=mapper)
    return df

# 五、财务-现金流量表
def fin_cashflow(pro, ts_code, data_len):
    mapper = {
        "ts_code": "TS股票代码",
        "ann_date": "公告日期",
        "f_ann_date": "实际公告日期",
        "end_date": "报告期",
        "comp_type": "公司类型(1一般工商业2银行3保险4证券)",
        "report_type": "报表类型",
        "end_type": "报告期类型",
        "net_profit": "净利润",
        "finan_exp": "财务费用",
        "c_fr_sale_sg": "销售商品、提供劳务收到的现金",
        "recp_tax_rends": "收到的税费返还",
        "n_depos_incr_fi": "客户存款和同业存放款项净增加额",
        "n_incr_loans_cb": "向中央银行借款净增加额",
        "n_inc_borr_oth_fi": "向其他金融机构拆入资金净增加额",
        "prem_fr_orig_contr": "收到原保险合同保费取得的现金",
        "n_incr_insured_dep": "保户储金净增加额",
        "n_reinsur_prem": "收到再保业务现金净额",
        "n_incr_disp_tfa": "处置交易性金融资产净增加额",
        "ifc_cash_incr": "收取利息和手续费净增加额",
        "n_incr_disp_faas": "处置可供出售金融资产净增加额",
        "n_incr_loans_oth_bank": "拆入资金净增加额",
        "n_cap_incr_repur": "回购业务资金净增加额",
        "c_fr_oth_operate_a": "收到其他与经营活动有关的现金",
        "c_inf_fr_operate_a": "经营活动现金流入小计",
        "c_paid_goods_s": "购买商品、接受劳务支付的现金",
        "c_paid_to_for_empl": "支付给职工以及为职工支付的现金",
        "c_paid_for_taxes": "支付的各项税费",
        "n_incr_clt_loan_adv": "客户贷款及垫款净增加额",
        "n_incr_dep_cbob": "存放央行和同业款项净增加额",
        "c_pay_claims_orig_inco": "支付原保险合同赔付款项的现金",
        "pay_handling_chrg": "支付手续费的现金",
        "pay_comm_insur_plcy": "支付保单红利的现金",
        "oth_cash_pay_oper_act": "支付其他与经营活动有关的现金",
        "st_cash_out_act": "经营活动现金流出小计",
        "n_cashflow_act": "经营活动产生的现金流量净额",
        "oth_recp_ral_inv_act": "收到其他与投资活动有关的现金",
        "c_disp_withdrwl_invest": "收回投资收到的现金",
        "c_recp_return_invest": "取得投资收益收到的现金",
        "n_recp_disp_fiolta": "处置固定资产、无形资产和其他长期资产收回的现金净额",
        "n_recp_disp_sobu": "处置子公司及其他营业单位收到的现金净额",
        "stot_inflows_inv_act": "投资活动现金流入小计",
        "c_pay_acq_const_fiolta": "购建固定资产、无形资产和其他长期资产支付的现金",
        "c_paid_invest": "投资支付的现金",
        "n_disp_subs_oth_biz": "取得子公司及其他营业单位支付的现金净额",
        "oth_pay_ral_inv_act": "支付其他与投资活动有关的现金",
        "n_incr_pledge_loan": "质押贷款净增加额",
        "stot_out_inv_act": "投资活动现金流出小计",
        "n_cashflow_inv_act": "投资活动产生的现金流量净额",
        "c_recp_borrow": "取得借款收到的现金",
        "proc_issue_bonds": "发行债券收到的现金",
        "oth_cash_recp_ral_fnc_act": "收到其他与筹资活动有关的现金",
        "stot_cash_in_fnc_act": "筹资活动现金流入小计",
        "free_cashflow": "企业自由现金流量",
        "c_prepay_amt_borr": "偿还债务支付的现金",
        "c_pay_dist_dpcp_int_exp": "分配股利、利润或偿付利息支付的现金",
        "incl_dvd_profit_paid_sc_ms": "其中:子公司支付给少数股东的股利、利润",
        "oth_cashpay_ral_fnc_act": "支付其他与筹资活动有关的现金",
        "stot_cashout_fnc_act": "筹资活动现金流出小计",
        "n_cash_flows_fnc_act": "筹资活动产生的现金流量净额",
        "eff_fx_flu_cash": "汇率变动对现金的影响",
        "n_incr_cash_cash_equ": "现金及现金等价物净增加额",
        "c_cash_equ_beg_period": "期初现金及现金等价物余额",
        "c_cash_equ_end_period": "期末现金及现金等价物余额",
        "c_recp_cap_contrib": "吸收投资收到的现金",
        "incl_cash_rec_saims": "其中:子公司吸收少数股东投资收到的现金",
        "uncon_invest_loss": "未确认投资损失",
        "prov_depr_assets": "加:资产减值准备",
        "depr_fa_coga_dpba": "固定资产折旧、油气资产折耗、生产性生物资产折旧",
        "amort_intang_assets": "无形资产摊销",
        "lt_amort_deferred_exp": "长期待摊费用摊销",
        "decr_deferred_exp": "待摊费用减少",
        "incr_acc_exp": "预提费用增加",
        "loss_disp_fiolta": "处置固定、无形资产和其他长期资产的损失",
        "loss_scr_fa": "固定资产报废损失",
        "loss_fv_chg": "公允价值变动损失",
        "invest_loss": "投资损失",
        "decr_def_inc_tax_assets": "递延所得税资产减少",
        "incr_def_inc_tax_liab": "递延所得税负债增加",
        "decr_inventories": "存货的减少",
        "decr_oper_payable": "经营性应收项目的减少",
        "incr_oper_payable": "经营性应付项目的增加",
        "others": "其他",
        "im_net_cashflow_oper_act": "经营活动产生的现金流量净额(间接法)",
        "conv_debt_into_cap": "债务转为资本",
        "conv_copbonds_due_within_1y": "一年内到期的可转换公司债券",
        "fa_fnc_leases": "融资租入固定资产",
        "im_n_incr_cash_equ": "现金及现金等价物净增加额(间接法)",
        "net_dism_capital_add": "拆出资金净增加额",
        "net_cash_rece_sec": "代理买卖证券收到的现金净额(元)",
        "credit_impa_loss": "信用减值损失",
        "use_right_asset_dep": "使用权资产折旧",
        "oth_loss_asset": "其他资产减值损失",
        "end_bal_cash": "现金的期末余额",
        "beg_bal_cash": "减:现金的期初余额",
        "end_bal_cash_equ": "加:现金等价物的期末余额",
        "beg_bal_cash_equ": "减:现金等价物的期初余额",
        "update_flag": "更新标志(1最新）",
    }

    # 获取开始时间与结束时间
    start_date, end_date = get_quarters(data_len)
    # 获取数据
    df = pro.cashflow(ts_code=ts_code, start_date=start_date, end_date=end_date)
    # 若有更新，则取最新值
    df = df.sort_values(by='update_flag', ascending=False).groupby("end_date").first().reset_index()
    # 去除一整列皆为空的列
    df = df.loc[:, df.count() > 0]
    # 把Nan处理为0
    df = df.fillna(0)
    # 重名数据字段
    df = df.rename(columns=mapper)
    return df


# 六、估值分位
def valuation_percentile(pro, ts_code, data_len):
    mapper = {
        "pe_ttm": "市盈率",
        "pb": "市净率",
        "ps_ttm": "市销率",
        "dv_ttm": "股息率",
    }

    # 获取开始时间与结束时间
    start_date, end_date = get_dates(data_len)
    # 指定字段
    fields = [
        "ts_code",
        "trade_date",
        "pe_ttm",
        "pb",
        "ps_ttm",
        "dv_ttm"
    ]
    # 获取数据
    df = pro.stk_factor_pro(ts_code=ts_code, start_date=start_date, end_date=end_date, fields=fields)
    # 重名数据字段
    df = df.rename(columns=mapper)
    # 空值使用零代替
    df = df.fillna(0)

    df_pe = valuation_bank(df, "市盈率", 252)
    df_pb = valuation_bank(df, "市净率", 252)
    df_ps = valuation_bank(df, "市销率", 252)
    df_dv = valuation_bank(df, "股息率", 252)

    return df_pe, df_pb, df_ps, df_dv

# 七、主营业务
def main_business(pro, ts_code, data_len):
    mapper = {
        "ts_code":"TS代码",
        "end_date":"报告期",
        "bz_item":"主营业务来源",
        "bz_code":"项目代码",
        "bz_sales":"主营业务收入(元)",
        "curr_type":"货币代码",
        "update_flag":"是否更新"
    }

    # 获取开始时间与结束时间
    start_date, end_date = get_quarters(data_len)
    # 指定字段
    fields = [
        "ts_code",
        "end_date",
        "bz_item",
        "bz_code",
        "bz_sales",
        "curr_type",
        "update_flag"
    ]
    # 获取数据
    df = pro.fina_mainbz(ts_code=ts_code, start_date=start_date, end_date=end_date, fields=fields)
    # 空值使用零代替
    df = df.fillna(0)
    # 聚合并筛选update_flag
    group_by_list = ["ts_code", "end_date", "bz_code", "bz_item", "curr_type"]
    df = df.sort_values(by='update_flag', ascending=False).groupby(group_by_list).first().reset_index()

    # 产品划分主营业务
    df_P = df[df['bz_code'] == 'P']

    # 地区划分主营业务
    df_D = df[df['bz_code'] == 'D']

    # 重名数据字段
    df_P = df_P.rename(columns=mapper)
    df_D = df_D.rename(columns=mapper)

    return df_P, df_D

# 八、行业排名
def industry_rank(pro, ts_code):
    mapper = {
        "trade_date": "交易日期",
        "ts_code": "TS股票代码",
        "name": "股票名称",
        "industry": "行业",
        "area": "地域",
        "pe": "市盈率（动）",
        "float_share": "流通股本（亿）",
        "total_share": "总股本（亿）",
        "total_assets": "总资产（亿）",
        "liquid_assets": "流动资产（亿）",
        "fixed_assets": "固定资产（亿）",
        "reserved": "公积金",
        "reserved_pershare": "每股公积金",
        "eps": "每股收益",
        "bvps": "每股净资产",
        "pb": "市净率",
        "list_date": "上市日期",
        "undp": "未分配利润",
        "per_undp": "每股未分配利润",
        "rev_yoy": "收入同比（%）",
        "profit_yoy": "利润同比（%）",
        "gpr": "毛利率（%）",
        "npr": "净利润率（%）",
        "holder_num": "股东人数"
    }
    # 获取当前日期，转换成为YYYYMMDD格式字符串
    now_date = datetime.now().strftime("%Y%m%d")
    # 获取上一个交易日
    last_trade = get_last_trade_date(pro, now_date)
    # 获取上一个交易日的全部信息（数据量等于当天上市公司数据）
    fields = [
        "ts_code","name","industry","pe","float_share","total_share",
        "total_assets","liquid_assets","fixed_assets","reserved",
        "reserved_pershare","eps","bvps","pb","undp",
        "per_undp","rev_yoy","profit_yoy","gpr","npr","holder_num"
    ]
    # 获取数据
    df_all = pro.bak_basic(trade_date=last_trade, fields=fields)

    # 根据股票代码获取申万行业第三行业分类代码
    l3_code = pro.index_member_all(ts_code=ts_code, fields=["l3_code"])['l3_code'].iloc[-1]

    # 根据第三行业分类代码，找到该分类下的全部对象
    l3_df = pro.index_member_all(l3_code=l3_code, fields=["l3_code", "l3_name", "ts_code", "name"])

    # 根据行业分类筛选出排名数据
    df_all = df_all[df_all['ts_code'].isin(l3_df['ts_code'])]

    # 选出需要进行排名的字段
    # rank_cols = [
    #     "pe", "float_share", "total_share","total_assets", "liquid_assets",
    #     "fixed_assets", "reserved", "reserved_pershare", "eps", "bvps", "pb",
    #     "undp", "per_undp", "rev_yoy", "profit_yoy", "gpr", "npr", "holder_num"
    # ]

    # 重名数据字段
    df_all = df_all.rename(columns=mapper)

    # for col in rank_cols:
    #     df_all[mapper[col]+"排名"] = df_all[mapper[col]].rank(method='first', ascending=False)

    return df_all

# 九、前十大股东+流动股东
def top10_holders(pro, ts_code):
    mapper = {
        "ts_code":"TS股票代码",
        "ann_date":"公告日期",
        "end_date":"报告期",
        "holder_name":"股东名称",
        "hold_amount":"持有数量（股）",
        "hold_ratio":"占总股本比例(%)",
        "hold_float_ratio":"占流通股本比例(%)",
        "hold_change":"持股变动",
        "holder_type":"股东类型"
    }
    # 获取数据
    df_top10_holders = pro.top10_holders(ts_code=ts_code, limit="10")
    df_top10_floatholders = pro.top10_floatholders(ts_code=ts_code, limit="10")

    # 重名数据字段
    df_top10_holders = df_top10_holders.rename(columns=mapper)
    df_top10_floatholders = df_top10_floatholders.rename(columns=mapper)

    return df_top10_holders, df_top10_floatholders


# 十、财务审计意见
def fina_audit(pro, ts_code, data_len):
    mapper = {
        "ts_code": "TS股票代码",
        "ann_date": "公告日期",
        "end_date": "报告期",
        "audit_result": "审计结果",
        "audit_fees": "审计总费用（元）",
        "audit_agency": "会计事务所",
        "audit_sign": "签字会计师"
    }
    # 获取开始时间与结束时间
    start_date, end_date = get_years(data_len)
    # 指定字段
    fields = [
        "ts_code",
        "ann_date",
        "end_date",
        "audit_result",
        "audit_agency",
        "audit_sign",
        "audit_fees"
    ]
    # 获取数据
    df = pro.fina_audit(ts_code=ts_code, start_date=start_date, end_date=end_date, fields=fields)
    # 数据去重
    df.drop_duplicates(inplace=True)
    # 数据重命名
    df = df.rename(columns=mapper)
    return df


if __name__ == '__main__':
    # 获取通道
    token = "32fd1a04e8f74b0ce8f1b8f7230cdec371aeb4a1b2efd967544eca63"
    pro = ts.pro_api(token)
    ts_code = "600521.SH" # 例子
    # 一、个股基础信息
    print(basic_info(pro, ts_code))
    basic_info(pro, ts_code).to_excel('stock.xlsx', index=False)

    # 二、筹码分布
    # data_len = 366
    # print(daily_chips_and_winrate(pro, ts_code, data_len=data_len))
    # daily_chips_and_winrate(pro, ts_code, data_len=data_len).to_excel('daily_chips_and_winrate.xlsx', index=False)

    # 三、财务-利润表
    # data_len = 8
    # print(fin_income(pro, ts_code, data_len))
    # fin_income(pro, ts_code, data_len).to_excel('fin_income.xlsx', index=False)

    # 四、财务-利润表
    # data_len = 8
    # print(fin_balancesheet(pro, ts_code, data_len))
    # fin_balancesheet(pro, ts_code, data_len).to_excel('fin_balancesheet.xlsx', index=False)

    # 五、财务-现金流量表
    # data_len = 8
    # print(fin_cashflow(pro, ts_code, data_len))
    # fin_cashflow(pro, ts_code, data_len).to_excel('fin_cashflow.xlsx', index=False)

    # 六、估值分位
    # data_len = 1024
    # print(valuation_percentile(pro, ts_code, data_len))
    # with pd.ExcelWriter('valuation_percentile.xlsx') as writer:
    #     # 保存到Excel文件的不同工作表
    #     for index, df in enumerate(valuation_percentile(pro, ts_code, data_len)):
    #         df.to_excel(writer, sheet_name=f'Sheet{index+1}', index=False)

    # 七、主营业务
    # data_len = 5
    # print(main_business(pro, ts_code, data_len))
    # with pd.ExcelWriter('main_business.xlsx') as writer:
    #     # 保存到Excel文件的不同工作表
    #     for index, df in enumerate(main_business(pro, ts_code, data_len)):
    #         df.to_excel(writer, sheet_name=f'Sheet{index+1}', index=False)

    # 八、行业排名
    # print(industry_rank(pro, ts_code))
    # industry_rank(pro, ts_code).to_excel('industry_rank.xlsx', index=False)

    # 九、前十大股东+流动股东
    # print(top10_holders(pro, ts_code))
    # with pd.ExcelWriter('top10_holders.xlsx') as writer:
    #     # 保存到Excel文件的不同工作表
    #     for index, df in enumerate(top10_holders(pro, ts_code)):
    #         df.to_excel(writer, sheet_name=f'Sheet{index+1}', index=False)

    # 十、财务审计意见
    # data_len = 6
    # print(fina_audit(pro, ts_code, data_len))
    # fina_audit(pro, ts_code, data_len).to_excel('fina_audit.xlsx', index=False)