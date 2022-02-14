require('dotenv').config();
const mysql = require('mysql2');
const fetch = require('node-fetch');
const { database } = require('./keys');
const conexion = mysql.createConnection({
    host: database.host,
    user: database.user,
    password: database.password,
    port: database.port,
    database: database.database
});

exports.handler = async function (event) {
    const promise = new Promise(async function() {
        var sql = `SELECT name FROM ${process.env.TABLE_TICKERS_LIST}`;
        conexion.query(sql, function (err, resultado) {
            if (err) throw err;
            guardarIncomeStatement(resultado);
        });
        async function guardarIncomeStatement(resultado){
            for (let i = 0; i < resultado.length; i++) {
                var ticker = resultado[i].name;
                await fetch(`https://financialmodelingprep.com/api/v3/income-statement/${ticker}?apikey=${process.env.FMP_KEY}`)
                .then((res) => {
                    return res.json();
                }).then((json) => {
                    var incomeStatement = json;
                    guardarBaseDeDatos(incomeStatement);
                }).catch((err) => {
                    console.error(err);
                });
            }
            await finalizarEjecucion();
        };
        function guardarBaseDeDatos(datos){
            for (let i = 0; i < datos.length; i++) {
                var sql = `INSERT INTO ${process.env.TABLE_INCOME_STATEMENT} (
                    date,
                    symbol, 
                    cik,
                    reportedCurrency,
                    fillingDate,
                    acceptedDate,
                    calendarYear,
                    period,
                    revenue,
                    costOfRevenue,
                    grossProfit,
                    grossProfitRatio,
                    researchAndDevelopmentExpenses,
                    generalAndAdministrativeExpenses,
                    sellingAndMarketingExpenses,
                    sellingGeneralAndAdministrativeExpenses,
                    otherExpenses,
                    operatingExpenses,
                    costAndExpenses,
                    interestIncome,
                    interestExpense,
                    depreciationAndAmortization,
                    ebitda,
                    ebitdaratio,
                    operatingIncome,
                    operatingIncomeRatio,
                    totalOtherIncomeExpensesNet,
                    incomeBeforeTax,
                    incomeBeforeTaxRatio,
                    incomeTaxExpense,
                    netIncome,
                    netIncomeRatio,
                    eps,
                    epsdiluted,
                    weightedAverageShsOut,
                    weightedAverageShsOutDil,
                    link,
                    finalLink
                    )
                    SELECT * FROM (SELECT
                        '${datos[i].date}' AS date,
                        '${datos[i].symbol}' AS symbol,
                        '${datos[i].cik}' AS cik,
                        '${datos[i].reportedCurrency}' AS reportedCurrency,
                        '${datos[i].fillingDate}' AS fillingDate,
                        '${datos[i].acceptedDate}' AS acceptedDate,
                        '${datos[i].calendarYear}' AS calendarYear,
                        '${datos[i].period}' AS period,
                        ${datos[i].revenue} AS revenue,
                        ${datos[i].costOfRevenue} AS costOfRevenue,
                        ${datos[i].grossProfit} AS grossProfit,
                        ${datos[i].grossProfitRatio} AS grossProfitRatio,
                        ${datos[i].researchAndDevelopmentExpenses} AS researchAndDevelopmentExpenses,
                        ${datos[i].generalAndAdministrativeExpenses} AS generalAndAdministrativeExpenses,
                        ${datos[i].sellingAndMarketingExpenses} AS sellingAndMarketingExpenses,
                        ${datos[i].sellingGeneralAndAdministrativeExpenses} AS sellingGeneralAndAdministrativeExpenses,
                        ${datos[i].otherExpenses} AS otherExpenses,
                        ${datos[i].operatingExpenses} AS operatingExpenses,
                        ${datos[i].costAndExpenses} AS costAndExpenses,
                        ${datos[i].interestIncome} AS interestIncome,
                        ${datos[i].interestExpense} AS interestExpense,
                        ${datos[i].depreciationAndAmortization} AS depreciationAndAmortization,
                        ${datos[i].ebitda} AS ebitda,
                        ${datos[i].ebitdaratio} AS ebitdaratio,
                        ${datos[i].operatingIncome} AS operatingIncome,
                        ${datos[i].operatingIncomeRatio} AS operatingIncomeRatio,
                        ${datos[i].totalOtherIncomeExpensesNet} AS totalOtherIncomeExpensesNet,
                        ${datos[i].incomeBeforeTax} AS incomeBeforeTax,
                        ${datos[i].incomeBeforeTaxRatio} AS incomeBeforeTaxRatio,
                        ${datos[i].incomeTaxExpense} AS incomeTaxExpense,
                        ${datos[i].netIncome} AS netIncome,
                        ${datos[i].netIncomeRatio} AS netIncomeRatio,
                        ${datos[i].eps} AS eps,
                        ${datos[i].epsdiluted} AS epsdiluted,
                        ${datos[i].weightedAverageShsOut} AS weightedAverageShsOut,
                        ${datos[i].weightedAverageShsOutDil} AS weightedAverageShsOutDil,
                        '${datos[i].link}' AS link,
                        '${datos[i].finalLink}' AS finalLink
                    ) AS tmp
                    WHERE NOT EXISTS (
                        SELECT date, symbol FROM ${process.env.TABLE_INCOME_STATEMENT} WHERE date = '${datos[i].date}' AND symbol = '${datos[i].symbol}'
                    ) LIMIT 1`;
                conexion.query(sql, function (err, resultado) {
                    if (err) throw err;
                    console.log(resultado);
                });
            }
        };
        async function finalizarEjecucion() {
            conexion.end()
            res.send("Ejecutado");
        }
    });
    return promise;
};