(function () {

    $(document).ready(function () {
        let lastRefreshTime = new Date().getTime();

        let ws = startWebSocket("ws://" + location.host);
        ws.addListener("dataPush", res => {
            if (!res.data.length) return;

            let i = 0;
            let now = new Date().getTime();
            let lastestTime = res.data.map(item => item._2._2).reduce((item1, item2) => item1 > item2 ? item1 : item2);
            let dataDelay = now - moment(lastestTime, "YYYY_MM_DD__HH_mm_ss_SSS").toDate().getTime();
            let refreshDelta = now - lastRefreshTime;
            lastRefreshTime = now;
            let lastTimeHtml = `
            <div class="row" style="margin-top: 24px; text-align: right;">
                <p>Last Refresh Time: ${moment().format("YYYY_MM_DD__HH_mm_ss_SSS")}</p>
                <p>Lastest Time Of Data: ${lastestTime}</p>
                <p>Data Delay: ${parseInt(dataDelay / 1000)} 秒 ${dataDelay % 1000} 毫秒</p>
                <p>Refresh Delta: ${parseInt(refreshDelta / 1000)} 秒 ${refreshDelta % 1000} 毫秒</p>  
            </div>
            `;

            let tableHtml = `
             <table class="table table-hover">
              <thead>
                <tr>
                  <th>#</th>
                  <th>Ip</th>
                  <th>Count</th>
                  <th>Last Time</th>
                </tr>
              </thead>
              <tbody>
                ${res.data.map(item => `
                <tr>
                  <th scope="row">${i++}</th>
                  <td>${item._1}</td>
                  <td>${item._2._1}</td>
                  <td>${item._2._2}</td>
                </tr>`).join("\n")}
              </tbody>
            </table>
            `;
            $("div.container").html(lastTimeHtml + tableHtml);
        })
    });

    function startWebSocket(address) {
        newInstance = new WebSocket(address);

        newInstance.onmessage = function (event) {
            if (event.data) {
                let data = JSON.parse(event.data);
                if (data.id) {
                    let callback = newInstance.callback[data.id];
                    if (callback && typeof callback === "function") {
                        callback(data);
                        delete newInstance.callback[data.id];
                        return;
                    }
                }

                if (data.key) {
                    let callback = newInstance.callback[data.key];
                    if (callback && typeof callback === "function") {
                        callback(data);
                    }
                }
            }
        };
        newInstance.onopen = (event) => {
            newInstance.connected = true;
        };
        newInstance.onclose = (event) => {
            newInstance.connected = false;
        };
        newInstance.onerror = (event) => {
            newInstance.close(1000);
        };

        newInstance.sendMessage = (msg, callback) => {
            if (msg !== null && msg.key && newInstance !== null && newInstance.connected) {
                msg.id = new Date().getTime() + "_" + parseInt(Math.random() * 9000 + 1000);
                newInstance.send(JSON.stringify(msg));
                addListener(msg.id, callback);
            }
        };

        newInstance.addListener = (idOrKey, callback) => {
            if (idOrKey !== null && typeof callback === "function") {
                newInstance.callback = newInstance.callback || {};
                newInstance.callback[idOrKey] = callback;
            }
        };

        return newInstance;
    }

})();