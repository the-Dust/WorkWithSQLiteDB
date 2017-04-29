using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SQLite;
using Microsoft.VisualBasic.FileIO;
using System.Globalization;
using System.Data;

namespace SQLiteCreation
{
    class Program
    {
        //Эти переменные определяют, в каком порядке находятся столбцы таблицы в исходном текстовом файле
        private static int idPosition;
        private static int dtPosition;
        private static int productIdPosition;
        private static int amountPosition;
        //Эти переменные служат для записи данных, считанных из текстовой таблицы построчно
        private static int id = 0;
        private static DateTime dt;
        private static int productId = 0;
        private static float amount = 0;
        //Эта переменная задает нижний предел даты в таблице. С ним будут сравниваться на валидность считанные данные столбца "dt"
        private static DateTime startDate = new DateTime(1970, 1, 1);
        //Эта переменная определяет начало заполнения базы данных. Служит для подсчета времени заполнения базы.
        private static DateTime startOfProcess;
        //Хэшсет служит для того, чтобы проверять на уникальность считанные данные столбца "id"
        private static HashSet<int> idSet = new HashSet<int>();
        //В этой строке хранятся все сообщения об ошибках для строки, считываемой из текстовой таблицы
        private static string errorMessage = "";

        static void Main(string[] args)
        {
            Console.WriteLine("------------------------------------------------------------------");
            Console.WriteLine("Программа запущена");
            Console.WriteLine("------------------------------------------------------------------");

            //Путь к текстовому файлу
            string pathToFile;
            try
            {
                pathToFile = args[0];
            }
            catch
            {
                Console.WriteLine("Вы не указали текстовый файл для загрузки данных");
                ClosingOnError();
                return;
            }

            //Создаем парсер, пробуем обратиться к текстовому файлу
            TextFieldParser csvReader;
            try
            {
                csvReader = new TextFieldParser(pathToFile);
            }
            catch (Exception ex)
            {
                Console.WriteLine("При обращении к текстовому файлу возникла ошибка.{0}Подробности:", Environment.NewLine);
                Console.WriteLine(ex.Message);
                ClosingOnError();
                return;
            }
            
            //Настраиваем парсер
            csvReader.SetDelimiters(new string[] { "\t" });
            csvReader.HasFieldsEnclosedInQuotes = true;

            //В эту строку будем записывать считанные данные
            string[] parsedString;
            
            //Считываем из таблицы первую строку с заголовками и ищем в ней названия столбцов
            parsedString = csvReader.ReadFields();
            idPosition = Array.IndexOf(parsedString, "id");
            dtPosition = Array.IndexOf(parsedString, "dt");
            productIdPosition = Array.IndexOf(parsedString, "product_id");
            amountPosition = Array.IndexOf(parsedString, "amount");

            //Ставим в соответствие название столбца и его позицию в таблице.
            //Если какого-то столбца нет, то прекращаем работу программы,
            //и с помощью словаря показываем, каких именно столбцов не хватает
            Dictionary<string, int> columnNameAndPosition = new Dictionary<string, int>() { { "id", idPosition }, { "dt", dtPosition }, { "product_id", productIdPosition }, { "amount", amountPosition } } ;
            if (columnNameAndPosition.ContainsValue(-1))
            {
                Console.WriteLine("В текстовом файле отсутствуют следующие столбцы:");
                string[] missingColumns = columnNameAndPosition.Where(x => x.Value == -1).Select(x => x.Key).ToArray();
                Console.WriteLine(string.Join(Environment.NewLine, missingColumns));
                ClosingOnError();
                return;
            }

            //Пробуем создать базу данных
            try
            {
                SQLiteConnection.CreateFile("MyDatabase.sqlite");
            }
            catch (Exception ex)
            {
                Console.WriteLine("При создании базы данных возникла ошибка.{0}Подробности:", Environment.NewLine);
                Console.WriteLine(ex.Message);
                ClosingOnError();
                return;
            }

            SQLiteConnection dbConnection;
            SQLiteCommand command;
            try
            {
                //Создаем подключение и подключаемся к базе
                dbConnection = new SQLiteConnection("Data Source=MyDatabase.sqlite;Version=3;");
                dbConnection.Open();

                //Создаем и заполняем таблицу product по условию задачи
                string sql = "CREATE TABLE product (id int primary key not null, name text not null)";
                command = new SQLiteCommand(sql, dbConnection);
                command.ExecuteNonQuery();

                sql = "insert into product values (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D'), (5, 'E'), (6, 'F'), (7, 'G');";
                command = new SQLiteCommand(sql, dbConnection);
                command.ExecuteNonQuery();
                
                //Создаем и заполняем таблицу order
                sql = "CREATE TABLE 'order' (id int primary key not null, dt datetime not null, product_id int not null, amount real not null, foreign key(product_id) references product(id))";
                command = new SQLiteCommand(sql, dbConnection);
                command.ExecuteNonQuery();

                Console.WriteLine("Начинаем добавлять данные в таблицу");
                Console.WriteLine();
                Console.WriteLine("Добавлено строк: 0");
                startOfProcess = DateTime.Now;

                //Задаем размер пачки строк, которые будем вставлять в базу
                int cycleSize = 250000;
                //Счетчик циклов, сколько пачек строк уже добавили
                int numberOfCycles = 0;
                //Количество валидных строк в текущем цикле
                int stepOfCurrentCycle = 0;
                //Счетчик всех считанных строк, нужен чтобы отображать, в какой строке текстового файла присутствует ошибка
                int sourceCounter = 0;

                StringBuilder sb = new StringBuilder("insert into 'order' (id, dt, product_id, amount) values ");
                while (!csvReader.EndOfData)
                {
                    try
                    {
                        parsedString = csvReader.ReadFields();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("В процессе чтения текстового файла возникла фатальная ошибка.{0}Подробности:", Environment.NewLine);
                        Console.WriteLine(ex.Message);
                        Console.WriteLine("База данных создана неполностью.");
                        ClosingOnError();
                        return;
                    }
                    sourceCounter++;

                    //Проверяем данные в считанной строке, при наличии ошибок выводим их на консоль и пишем в лог ошибок
                    if (!DataVerification(parsedString, sourceCounter, ref errorMessage))
                    {
                        using (System.IO.StreamWriter file = new System.IO.StreamWriter(string.Format(@"errorlog_{0}.txt", DateTime.Now.ToString(@"dd-MM-yyyy_HH-mm.ss")), true))
                        {
                            Console.WriteLine(errorMessage);
                            file.WriteLine(errorMessage);
                            //После вывода всех ошибок напоминаем, сколько добавлено строк
                            Console.WriteLine("\rДобавлено строк: {0}", numberOfCycles * cycleSize);
                        }
                        continue;
                    }

                    //Если в строке нет ошибок, добавляем данные из нее к sql запросу
                    stepOfCurrentCycle++;
                    sb.Append(string.Format("({0},'{1}',{2},{3}),", id, dt.ToString("s"), productId, amount.ToString("G", CultureInfo.InvariantCulture)));

                    //Как только набралась пачка строк заданного размера, добавляем ее
                    if (stepOfCurrentCycle == cycleSize)
                    {
                        string execute = sb.ToString().TrimEnd(',') + ";";
                        command = new SQLiteCommand(execute, dbConnection);
                        command.ExecuteNonQuery();
                        sb.Clear();
                        sb.Append("insert into 'order' (id, dt, product_id, amount) values ");
                        numberOfCycles++;
                        Console.Write("\rДобавлено строк: {0}", numberOfCycles * cycleSize);
                        stepOfCurrentCycle = 0;
                    }
                }

                //Добавляем оставшиеся последние строки
                if (stepOfCurrentCycle != 0)
                {
                    string execute = sb.ToString().TrimEnd(',') + ";";
                    command = new SQLiteCommand(execute, dbConnection);
                    command.ExecuteNonQuery();
                    sb.Clear();
                    Console.WriteLine("\rДобавлено строк: {0}", numberOfCycles * cycleSize + stepOfCurrentCycle);
                    stepOfCurrentCycle = 0;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("В процессе работы с базой данных возникла ошибка.{0}Подробности:", Environment.NewLine);
                Console.WriteLine(ex.Message);
                Console.WriteLine("База данных создана неполностью.");
                ClosingOnError();
                return;
            }

            Console.WriteLine("------------------------------------------------------------------");
            Console.WriteLine("Операция заполнения базы данных завершена успешно.{0}Время выполнения операции (мин:сек.сот): {1}", Environment.NewLine, (DateTime.Now - startOfProcess).ToString(@"mm\:ss\.ff"));
            Console.WriteLine("------------------------------------------------------------------");
            if (errorMessage != "")
            {
                Console.WriteLine("В процессе чтения текстового файла возникли ошибки.{0}Подробнее с ними можно ознакомиться в файле errorlog.txt", Environment.NewLine);
                Console.WriteLine("------------------------------------------------------------------");
            }

            Console.WriteLine("{0}ЗАПРОСЫ{0}", Environment.NewLine);

            //В этих строках хранятся запросы
            string query1 = "select product.name as `Продукт`, count(*) as `Кол-во заказов`, sum(`order`.amount) " +
                             "as `Сумма заказов`  from 'order' join product on `product`.id = `order`.product_id " +
                             "where strftime('%Y-%m', dt) = strftime('%Y-%m', 'now') group by product.name";

            string query2a = "select distinct product.name as `Продукт` from 'order' " +
                            "join product on `product`.id = `order`.product_id " +
                            "where strftime('%Y-%m', dt) = strftime('%Y-%m', 'now') " +
                            "except " +
                            "select distinct product.name as `Продукт` from 'order' " +
                            "join product on `product`.id = `order`.product_id " +
                            "where strftime('%Y-%m', dt) = strftime('%Y-%m', 'now', '-1 month')";

            string query2b = "select ifnull(name2, '') as `Прошлый месяц`, ifnull(name3, '') as `Текущий месяц` from product " +
                            "left outer join " +
                            "(select distinct `product`.id as id1, product.name as name2 from 'order' " +
                            "join product on `product`.id = `order`.product_id " +
                            "where strftime('%Y-%m', dt) = strftime('%Y-%m', 'now', '-1 month') " +
                            "except " +
                            "select distinct `product`.id as id1, product.name as name2 from 'order' " +
                            "join product on `product`.id = `order`.product_id " +
                            "where strftime('%Y-%m', dt) = strftime('%Y-%m', 'now')) " +
                            "on id1 = product.id " +
                            "left outer join " +
                            "(select distinct `product`.id as id2, product.name as name3 from 'order' " +
                            "join product on `product`.id = `order`.product_id " +
                            "where strftime('%Y-%m', dt) = strftime('%Y-%m', 'now') " +
                            "except " +
                            "select distinct `product`.id as id2, product.name as name3 from 'order' " +
                            "join product on `product`.id = `order`.product_id " +
                            "where strftime('%Y-%m', dt) = strftime('%Y-%m', 'now', '-1 month')) " +
                            "on id2 = product.id";

            string query3 = "select period as `Период`, product_name as `Продукт`, max(total_amount) as `Сумма`, round(max(total_amount)*100/sum(total_amount),2) as `Доля,%` " +
                            "from(select strftime('%Y-%m', dt) as period, sum(amount) as total_amount, product.name as product_name from `order` " +
                            "join product on `product`.id = `order`.product_id group by product.name, strftime('%Y-%m', dt) order by strftime('%Y-%m', dt) asc) group by period order by period asc; ";



            Console.WriteLine("1 Вывести количество и сумму заказов по каждому продукту за {0}текущий месяц{0}", Environment.NewLine);
            QueryToConsole(query1, dbConnection, false);
            Console.WriteLine();

            Console.WriteLine("2a Вывести все продукты, которые были заказаны в текущем {0}месяце, но которых не было в прошлом.{0}", Environment.NewLine);
            QueryToConsole(query2a, dbConnection, false);
            Console.WriteLine();

            Console.WriteLine("2b Вывести все продукты, которые были только в прошлом месяце,{0}но не в текущем, и которые были в текущем месяце, но не в прошлом.{0}", Environment.NewLine);
            QueryToConsole(query2b, dbConnection, false);
            Console.WriteLine();

            Console.WriteLine("3 Помесячно вывести продукт, по которому была максимальная сумма{0}заказов за этот период, сумму по этому продукту и его долю от{0}общего объема за этот период.{0}", Environment.NewLine);
            QueryToConsole(query3, dbConnection, true);
            Console.WriteLine();


            dbConnection.Close();
            Console.WriteLine("{0}Работа программы выполнена. Для выхода нажмите любую кнопку", Environment.NewLine);
            Console.ReadKey();
        }

        public static void QueryToConsole(string query, SQLiteConnection connection, bool treatData)
        {
            SQLiteCommand command = new SQLiteCommand(query, connection);
            DataTable table = new DataTable();

            //Загружаем в таблицу ответ на запрос к базе
            table.Load(command.ExecuteReader());

            //Выводим таблицу на консоль
            string[][] currentRows = table.Select().Select(x=> x.ItemArray.Select(y=>y.ToString()).ToArray()).ToArray();

            if (treatData)
            {
                //Приводим данные из вида "yyyy-MM" от SQLite к виду "MMM yyyy", который указан в задании
                currentRows = currentRows.Select(x => x.Select((e, i) => i == 0 ? DateTime.Parse(e).ToString("MMM yyyy") : e).ToArray()).OrderBy(x => DateTime.Parse(x[0])).ToArray();
            }
                
            if (currentRows.Count() < 1 || currentRows.All(y => string.IsNullOrWhiteSpace(string.Concat(y))))
                Console.WriteLine("Таблица не содержит строк");
            else
            {
                foreach (DataColumn column in table.Columns)
                    Console.Write(" {0, -15}|", column.ColumnName);
                Console.WriteLine("\t");

                //Начертим разделитель между строкой заголовка и данными
                int length = table.Columns.Count;
                string limiter = new string('-', length * 17);
                Console.WriteLine(limiter);

                foreach (var row in currentRows)
                {
                    foreach (string column in row)
                        Console.Write(" {0, -15}|", column);
                    Console.WriteLine("\t");
                }
            }
        }

        public static void ClosingOnError()
        {
            Console.WriteLine("Данное приложение будет закрыто. Нажмите любую кнопку для продолжения");
            Console.ReadKey();
            return;
        }

        public static bool DataVerification(string[] inputData, int counter, ref string message)
        {
            
            StringBuilder errorString = new StringBuilder();
            try
            {
                if (!int.TryParse(inputData[idPosition], out id))
                {
                    if (string.IsNullOrWhiteSpace(inputData[idPosition]))
                        errorString.Append("- Значение id не указано" + Environment.NewLine);
                    else
                        errorString.Append("- id не является числом Int32" + Environment.NewLine);
                }
                else if (id < 0)
                {
                    errorString.Append("- id имеет отрицательное значение" + Environment.NewLine);
                }
                else if (!idSet.Add(id))
                {
                    errorString.Append("- id имеет неуникальное значение" + Environment.NewLine);
                }
                if (!DateTime.TryParse(inputData[dtPosition], out dt))
                {
                    if (string.IsNullOrWhiteSpace(inputData[dtPosition]))
                        errorString.Append("- Значение dt не указано" + Environment.NewLine);
                    else
                        errorString.Append("- dt имеет неверный формат даты" + Environment.NewLine);
                }
                else if (dt < startDate)
                {
                    errorString.Append("- Указана дата ранее 1970-01-01 (по условиям задачи)" + Environment.NewLine);
                }
                else if (dt > DateTime.Now)
                {
                    errorString.Append("- Указанная дата еще не наступила" + Environment.NewLine);
                }
                if (!int.TryParse(inputData[productIdPosition], out productId))
                {
                    if (string.IsNullOrWhiteSpace(inputData[productIdPosition]))
                        errorString.Append("- Значение productId не указано" + Environment.NewLine);
                    else
                        errorString.Append("- product_id не является числом Int32" + Environment.NewLine);
                }
                else if (productId < 1 || productId > 7)
                {
                    errorString.Append("- product_id не является значением id из таблицы \"product\"" + Environment.NewLine);
                }
                //Флаг того, что распознать значение amount не удалось
                bool amountFail = false;
                try
                {
                    amount = Single.Parse(inputData[amountPosition], CultureInfo.InvariantCulture);
                }
                catch
                {
                    amountFail = true;
                    if (string.IsNullOrWhiteSpace(inputData[amountPosition]))
                        errorString.Append("- Значение amount не указано" + Environment.NewLine);
                    else
                        errorString.Append("- amount не является числом real" + Environment.NewLine);
                }
                if (!amountFail & amount < 0)
                {
                    errorString.Append("- amount имеет отрицательное значение" + Environment.NewLine);
                }
                if (errorString.Length > 0)
                {
                    message = string.Format("*********{0}В строке {1} обнаружены следующие ошибки:{0}{2}Данная строка будет проигнорирована{0}", Environment.NewLine, counter, errorString);
                    return false;
                }
                else return true;
            }
            catch 
            {
                errorString.Append("- в строке содержатся не все данные" + Environment.NewLine);
                message = string.Format("*********{0}В строке {1} обнаружены следующие ошибки:{0}{2}Данная строка будет проигнорирована{0}", Environment.NewLine, counter, errorString);
                return false;
            }
        }
    }
}
