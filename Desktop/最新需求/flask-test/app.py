from flask_cors import *
from flask import render_template, send_from_directory, redirect, url_for,session
import re
from flask import Flask, request, jsonify, Response
import pymysql
import json
from datetime import datetime
from get_data import basic_info
#内网ip
app = Flask(__name__)
# app.secret_key = os.urandom(24)
# 更改 Jinja 分隔符
app.jinja_env.variable_start_string = '<<'
app.jinja_env.variable_end_string = '>>'
app.jinja_env.block_start_string = '[%'
app.jinja_env.block_end_string = '%]'
app.jinja_env.comment_start_string = '[#'
app.jinja_env.comment_end_string = '#]'
CORS(app, supports_credentials=True)
app.secret_key = 'your_secret_key'
conn = pymysql.connect(
            host="127.0.0.1",
            user="root",
            password="123456",
            database="fiance",
            charset="utf8mb4",  # 设置字符集为utf8mb4
            cursorclass = pymysql.cursors.DictCursor
        )



########################设置拦截器#############################3

@app.before_request
def before_request():
    url = request.path
    global session
    # 定义无需登录的路径列表
    pass_list = ['/', '/register', '/stock_list','/valid']

    # 如果请求的路径是登录页面、注册页面或静态资源，跳过拦截
    if url in pass_list or url.endswith('.js') or url.endswith('.css') or url.startswith('/static')or url.startswith('/stock_list'):
        return  # 正确终止此函数，继续处理请求

    # 检查用户是否已经登录（假设使用了 session）
    if 'user_id' not in session:
        print(session)
        # 如果用户没有登录，重定向到登录页面
        return redirect(url_for(''))

    # 其他预处理逻辑...
    print(f"Processing request for: {request.path}")

@app.route('/33')
def home():
    return "Home Page"
# 设置全局的响应拦截器
@app.after_request
def after_request(response):
    # 允许跨域
    response.headers['Access-Control-Allow-Origin'] = '*'
    # 添加其他头部或修改响应内容
    return response

@app.route('/static/js/<path:filename>')
def serve_static(filename):
    return send_from_directory('static/js', filename)
####################################################################页面渲染
@app.route('/')
def login():
    return  render_template('login.html')
@app.route('/register')
def register():
    return  render_template('register.html')
@app.route('/index')
def index():
    return  render_template('index.html')

import tushare as ts
@app.route('/basic_info')
def basic_info_templeate():
    query = request.args.get("query")  # 获取前端传来的查询参数
    token = "32fd1a04e8f74b0ce8f1b8f7230cdec371aeb4a1b2efd967544eca63"
    pro = ts.pro_api(token)
    ts_code = "600521.SH"  # 例子

    # 正则表达式提取股票代码
    stock_code_match = re.search(r'\b\d{6}\.\w{2}\b', query)
    stock_code = stock_code_match.group(0) if stock_code_match else None

    cursor = conn.cursor()

    try:
        if stock_code:
            sql = "SELECT * FROM basic_info WHERE Stock_Code = %s LIMIT 1"
            cursor.execute(sql, (stock_code,))
        else:
            sql = "SELECT * FROM basic_info WHERE company_name LIKE %s LIMIT 1"
            cursor.execute(sql, (f'%{query}%',))

        result = cursor.fetchall()
        print(result)

        # 将结果传递给模板
        return render_template('stock/basic_info.html', result=result)

    except Exception as e:
        print(f"An error occurred: {e}")
        return render_template('stock/basic_info.html', result=[])

    finally:
        cursor.close()
        conn.close()


@app.route('/candlesticku_sh')
def candlesticku_sh():
    query = request.args.get("query")  # 从前端获取查询参数
    cursor = None
    valid_result = []

    try:
        # 使用全局连接
        global conn
        cursor = conn.cursor(pymysql.cursors.DictCursor)  # 使用 DictCursor 以字典格式返回结果
        sql = "SELECT * FROM daily_stock_data WHERE ts_code = %s ORDER BY trade_date"
        conn.ping(reconnect=True)

        # 执行查询
        cursor.execute(sql, (query,))
        result = cursor.fetchall()

        for row in result:
            # 检查日期是否有效并转换格式
            try:
                trade_date = datetime.strptime(row['trade_date'], '%Y%m%d').strftime('%Y/%m/%d')
                valid_result.append([trade_date, row['open'], row['close'], row['low'], row['high']])
            except ValueError:
                print(f"Invalid date found: {row['trade_date']}, skipping this entry.")

        # 确保在循环外部返回数据
        return render_template('stock/candlestick_sh.html', result=valid_result)

    except pymysql.MySQLError as e:
        return jsonify([])  # 返回空列表表示发生错误

    finally:
        if cursor:
            cursor.close()



@app.route('/personal')
def personal():
    user_id = session.get('user_id')  # 从 session 中获取用户ID
    if not user_id:
        return "用户未登录或 session 无效", 401  # 如果没有用户ID，返回未授权

    # 初始化数据库连接
    try:
        if not conn.open:
            conn.connect()
        with conn.cursor() as cursor:
            # 使用参数化查询防止SQL注入
            sql = "SELECT * FROM user WHERE UserId = %s"
            cursor.execute(sql, (user_id,))
            result = cursor.fetchall()

            # 打印查询结果（仅调试用）
            print(result)
        return render_template('person/personal.html', result=result)

    except pymysql.MySQLError as e:
        # 捕获并处理数据库错误
        print(f"Database error: {e}")
        return jsonify([])  # 返回空列表表示出现错误

    finally:
        # 确保数据库连接在最后关闭
        conn.close()



@app.route('/admin_user')
def admin_user():
    return  render_template('admin/index.html',d=[])


@app.route('/search')
def search():
    return  render_template('search.html')





@app.route('/admin')
def admin():
    return  render_template('admin/admin.html')
####################################数据交互###################################

@app.route('/admin_data')
def admin_data():
    try:
        # Use global connection
        cursor = conn.cursor()
        # Correct SQL query with parameterized format
        sql = "SELECT * FROM user"
        conn.ping(reconnect=True)
        # Execute the query with the appropriate parameter
        cursor.execute(sql)
        conn.commit()
        result = cursor.fetchall()

        # 构造响应数据
        # 构造响应数据
        result_list = {
            "code": 0,  # 添加缺少的值
            "msg": "",  # 添加空消息字段
            "count": len(result),  # 获取行数
            "data": result  # 直接传递结果集
        }

        # 返回JSON响应
        return jsonify(result_list)

    except pymysql.MySQLError as e:
        print(f"Database error: {e}")
        return jsonify([])  # Return an empty list indicating an error

    finally:
        if cursor:
            cursor.close()
# Assuming 'conn' is your global database connection object
# Make sure to initialize 'conn' properly



@app.route('/candlesticku_sh_data')
def candlesticku_sh_data():
    query = request.args.get("query")  # Get query parameter from the frontend
    cursor = None
    try:
        # Use global connection
        global conn
        cursor = conn.cursor()

        # Correct SQL query with parameterized format
        sql = "SELECT * FROM daily_stock_data WHERE ts_code = %s ORDER BY trade_date"
        conn.ping(reconnect=True)
        print(sql)

        # Execute the query with the appropriate parameter
        cursor.execute(sql, ('600519.SH',))
        conn.commit()

        result = cursor.fetchall()
        valid_result = []
        for row in result:
            print(row)
            # Check if the date is valid
            try:
                trade_date = datetime.strptime(row['trade_date'], '%Y%m%d').strftime('%Y/%m/%d')
                valid_result.append([trade_date, row['open'], row['close'], row['low'], row['high']])
            except ValueError:
                print(f"Invalid date found: {row['trade_date']}, skipping this entry.")

        print(valid_result)

        # Return JSON response
        return jsonify(valid_result)

    except pymysql.MySQLError as e:
        print(f"Database error: {e}")
        return jsonify([])  # Return an empty list indicating an error

    finally:
        if cursor:
            cursor.close()





@app.route('/stock_list', methods=['GET'])
def receive_data():
    query = request.args.get("query")  # 获取前端传来的查询参数
    print(query)
    cursor = None
    try:
        # 使用全局连接
        global conn
        cursor = conn.cursor()
        # 查询数据表数据
        sql = "SELECT * FROM basic_info"
        conn.ping(reconnect=True)
        cursor.execute(sql)
        conn.commit()
        result = cursor.fetchall()
        # 只保留 'company_name' 和 'Stock_Code' 字段
        result = [f"{row['company_name']}[{row['Stock_Code']}]"
                  for row in result]
        print(result)
        # 返回JSON响应
        return Response(json.dumps(result), mimetype='application/json')

    except pymysql.MySQLError as e:
        print(f"Database error: {e}")
        return jsonify([])  # 返回空列表表示出现错误

    finally:
        if cursor:
            cursor.close()
        # 注意：连接不在这里关闭


@app.route('/user_edit', methods=['GET', 'POST'])
def user_edit():
    # 获取查询参数（例如，通过 GET 方法）
    homepage = request.form.get('homepage', '0')
    stocks = request.form.get('stocks', '0')
    industry = request.form.get('industry', '0')
    macro = request.form.get('macro', '0')
    futures = request.form.get('futures', '0')
    options = request.form.get('options', '0')
    quantitative = request.form.get('quantitative', '0')
    ##////////////////
    UserId = request.args.get("UserId")
    # 获取表单数据（例如，通过 POST 方法）
    email = request.args.get('Email')
    password = request.args.get('Password')
    phone = request.args.get('Phone')
    username = request.args.get('UserName')
    ban = request.args.get('ban', type=int)  # 假设ban为整数类型
    cursor = None
    try:
        cursor = conn.cursor()

        # 检查是查询还是更新操作
        if request.method == 'GET':
            # 更新用户信息的 SQL 查询
            sql = """
                UPDATE user 
                SET email = %s, password = %s, phone = %s, username = %s
                WHERE UserId = %s
            """
            conn.ping(reconnect=True)
            cursor.execute(sql, (email, password, phone, username,UserId))
            conn.commit()

            # 成功更新，返回响应
            return jsonify({"message": "User updated successfully"})

        elif request.method == 'POST':
            # 更新用户信息的 SQL 查询
            sql = """
                UPDATE user 
                SET email = %s, password = %s, phone = %s, username = %s,
                WHERE UserId = %s 
            """
            conn.ping(reconnect=True)
            cursor.execute(sql, (email, password, phone, username, UserId))
            conn.commit()

            # 成功更新，返回响应
            return jsonify({"message": "User updated successfully"})

    except pymysql.MySQLError as e:
        print(f"Database error: {e}")
        return jsonify({"message": "An error occurred while accessing the database"}), 500

    finally:
        if cursor:
            cursor.close()
        # 注意：连接不在这里关闭


@app.route('/update_personal_info', methods=['POST'])
def update_personal_info():
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'success': False, 'message': '用户未登录'})

    # 获取前端提交的表单数据
    username = request.form.get('username')
    password = request.form.get('password')
    email = request.form.get('email')
    phone = request.form.get('phone')

    try:
        conn.ping(reconnect=True)  # 检查并重连数据库
        cursor = conn.cursor()

        # 更新用户信息
        sql = "UPDATE user SET UserName=%s, Password=%s, Email=%s, Phone=%s WHERE UserId=%s"
        cursor.execute(sql, (username, password, email, phone, user_id))
        conn.commit()
        return jsonify({'success': True})

    except pymysql.MySQLError as e:
        print(f"Database error: {e.args[0]}, {e.args[1]}")
        return jsonify({'success': False, 'message': str(e)})

    except Exception as e:
        print(f"Unexpected error: {e}")
        return jsonify({'success': False, 'message': 'An unexpected error occurred'})

    finally:
        if cursor:
            cursor.close()


@app.route('/admin_user_delete', methods=['GET'])
def admin_user_delete():
    items_json = request.args.get('items', '[]')
    items = json.loads(items_json)

    try:
        # 连接数据库
        cursor = conn.cursor()

        # 假设我们要根据UserName删除用户
        for item in items:
            sql = "DELETE FROM user WHERE UserName = %s"
            cursor.execute(sql, (item['UserName'],))

        conn.commit()

    except pymysql.MySQLError as e:
        print(f"Database error: {e}")
        return jsonify({"status": "error", "message": "Failed to delete records"}), 500

    finally:
        # 确保连接被关闭
        if conn and conn.open:
            conn.close()

    return jsonify({"status": "success", "message": "Records deleted successfully"})


@app.route("/valid")
def valid():
    UserName = request.args.get('username', '')
    Password = request.args.get('password', '')
    global  session
    cursor = None
    try:
        # 使用数据库连接
        global conn
        cursor = conn.cursor()
        sql = "SELECT * FROM user WHERE UserName = %s AND Password = %s"
        conn.ping(reconnect=True)

        # 执行查询，传递参数
        cursor.execute(sql, (UserName, Password))
        conn.commit()
        result = cursor.fetchall()

        # 构造响应数据
        result_list = {
            "code": 1 if result else 0,  # 结果存在时返回 1，否则返回 0
            "msg": "",  # 可根据实际情况设置消息
            "count": len(result),  # 获取行数
            "data": result  # 直接传递结果集
        }
        if(len(result)==1):
            session['user_id']=result[0]['UserId']
        return jsonify({"result": result_list})

    except pymysql.MySQLError as e:
        print(f"Database error: {e}")
        return jsonify({"status": "error", "message": "Failed to query records"}), 500

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route("/user_search")
def user_search():
    query = request.args.get('query', '')
    cursor = None
    try:
        # 使用数据库连接
        global conn
        cursor = conn.cursor()
        sql = "SELECT * FROM user WHERE UserName like %s OR Phone = %s OR Email = %s "
        conn.ping(reconnect=True)

        # 执行查询，传递参数
        cursor.execute(sql, (query,query,query))
        conn.commit()
        result = cursor.fetchall()
        # 构造响应数据
        result_list = {
            "code": 0 if result else 1,  # 结果存在时返回 1，否则返回 0
            "msg": "",  # 可根据实际情况设置消息
            "count": len(result),  # 获取行数
            "data": result  # 直接传递结果集
        }
        return jsonify({"result": result_list})

    except pymysql.MySQLError as e:
        print(f"Database error: {e}")
        return jsonify({"status": "error", "message": "Failed to query records"}), 500

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()




if __name__ == "__main__":
    """初始化,debug=True"""
    app.run(host='127.0.0.1', port=4000,debug=True)