require('dotenv').config();
const express = require('express');
const morgan = require('morgan');
const mysql = require('mysql2');
const fetch = require('node-fetch');
const { database } = require('./keys');
const PUERTO = 4300;
const app = express();
const conexion = mysql.createConnection({
    host: database.host,
    user: database.user,
    password: database.password,
    port: database.port,
    database: database.database
});

app.use(morgan('dev'));

app.get('/', async (req, res) => {
    var sql = `SELECT name FROM ${process.env.TABLE_TICKERS_LIST}`;
    conexion.query(sql, function (err, resultado) {
        if (err) throw err;
        guardarCashFlow(resultado);
    });
    async function guardarCashFlow(resultado){
        for (let i = 0; i < resultado.length; i++) {
            var ticker = resultado[i].name;
            await fetch(`https://financialmodelingprep.com/api/v3/cash-flow-statement/${ticker}?apikey=${process.env.FMP_KEY}`)
            .then((res) => {
                return res.json();
            }).then((json) => {
                var cashFlow = json;
                guardarBaseDeDatos(cashFlow);
            }).catch((err) => {
                console.error(err);
            });
        }
        await finalizarEjecucion();
    };
    function guardarBaseDeDatos(datos){
        for (let i = 0; i < datos.length; i++) {
            var sql = `INSERT INTO ${process.env.TABLE_CASH_FLOW} (
                date,
                symbol, 
                cik,
                reportedCurrency,
                fillingDate,
                acceptedDate,
                calendarYear,
                period,
                netIncome,
                depreciationAndAmortization,
                deferredIncomeTax,
                stockBasedCompensation,
                changeInWorkingCapital,
                accountsReceivables,
                inventory,
                accountsPayables,
                otherWorkingCapital,
                otherNonCashItems,
                netCashProvidedByOperatingActivities,
                investmentsInPropertyPlantAndEquipment,
                acquisitionsNet,
                purchasesOfInvestments,
                salesMaturitiesOfInvestments,
                otherInvestingActivites,
                netCashUsedForInvestingActivites,
                debtRepayment,
                commonStockIssued,
                commonStockRepurchased,
                dividendsPaid,
                otherFinancingActivites,
                netCashUsedProvidedByFinancingActivities,
                effectOfForexChangesOnCash,
                netChangeInCash,
                cashAtEndOfPeriod,
                cashAtBeginningOfPeriod,
                operatingCashFlow,
                capitalExpenditure,
                freeCashFlow,
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
                    ${datos[i].netIncome} AS netIncome,
                    ${datos[i].depreciationAndAmortization} AS depreciationAndAmortization,
                    ${datos[i].deferredIncomeTax} AS deferredIncomeTax,
                    ${datos[i].stockBasedCompensation} AS stockBasedCompensation,
                    ${datos[i].changeInWorkingCapital} AS changeInWorkingCapital,
                    ${datos[i].accountsReceivables} AS accountsReceivables,
                    ${datos[i].inventory} AS inventory,
                    ${datos[i].accountsPayables} AS accountsPayables,
                    ${datos[i].otherWorkingCapital} AS otherWorkingCapital,
                    ${datos[i].otherNonCashItems} AS otherNonCashItems,
                    ${datos[i].netCashProvidedByOperatingActivities} AS netCashProvidedByOperatingActivities,
                    ${datos[i].investmentsInPropertyPlantAndEquipment} AS investmentsInPropertyPlantAndEquipment,
                    ${datos[i].acquisitionsNet} AS acquisitionsNet,
                    ${datos[i].purchasesOfInvestments} AS purchasesOfInvestments,
                    ${datos[i].salesMaturitiesOfInvestments} AS salesMaturitiesOfInvestments,
                    ${datos[i].otherInvestingActivites} AS otherInvestingActivites,
                    ${datos[i].netCashUsedForInvestingActivites} AS netCashUsedForInvestingActivites,
                    ${datos[i].debtRepayment} AS debtRepayment,
                    ${datos[i].commonStockIssued} AS commonStockIssued,
                    ${datos[i].commonStockRepurchased} AS commonStockRepurchased,
                    ${datos[i].dividendsPaid} AS dividendsPaid,
                    ${datos[i].otherFinancingActivites} AS otherFinancingActivites,
                    ${datos[i].netCashUsedProvidedByFinancingActivities} AS netCashUsedProvidedByFinancingActivities,
                    ${datos[i].effectOfForexChangesOnCash} AS effectOfForexChangesOnCash,
                    ${datos[i].netChangeInCash} AS netChangeInCash,
                    ${datos[i].cashAtEndOfPeriod} AS cashAtEndOfPeriod,
                    ${datos[i].cashAtBeginningOfPeriod} AS cashAtBeginningOfPeriod,
                    ${datos[i].operatingCashFlow} AS operatingCashFlow,
                    ${datos[i].capitalExpenditure} AS capitalExpenditure,
                    ${datos[i].freeCashFlow} AS freeCashFlow,
                    '${datos[i].link}' AS link,
                    '${datos[i].finalLink}' AS finalLink
                ) AS tmp
                WHERE NOT EXISTS (
                    SELECT date, symbol FROM ${process.env.TABLE_CASH_FLOW} WHERE date = '${datos[i].date}' AND symbol = '${datos[i].symbol}'
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

app.listen(process.env.PORT || PUERTO, () => {
    console.log(`Escuchando en puerto ${process.env.PORT || PUERTO}`);
});