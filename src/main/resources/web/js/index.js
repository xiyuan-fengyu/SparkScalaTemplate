(function () {

    let curData = {
        ipCounts: null,
        agentKeyWords: null
    };

    $(document).ready(function () {
        let lastRefreshTime = new Date().getTime();

        let ws = startWebSocket("ws://" + location.host);
        ws.addListener("dataPush", res => {
            if (!res.data || !res.data.ipCounts) return;

            curData = res.data;
            computeAgentKeyWords();

            let i = 0;
            let tableHtml = `
             <table class="table table-hover">
              <thead>
                <tr>
                  <th>#</th>
                  <th>Ip</th>
                  <th>Count</th>
                  <th>Agent Key Words</th>
                </tr>
              </thead>
              <tbody>
                ${res.data.ipCounts.filter(item => item[1][0] >= 10).map(item => `
                <tr class="ipCountsItem" data-index="${i}">
                  <th scope="row">${i++}</th>
                  <td>${item[0]}</td>
                  <td>${item[1][0]}</td>
                  <td>${item[1][2].map(keyWord => `
                    <label>${keyWord}</label>
                  `)}</td>
                </tr>`).join("\n")}
              </tbody>
            </table>
            `;
            $("#ipCounts").html(tableHtml);

            $(".ipCountsItem").click(function () {
                showAgent(parseInt($(this).attr("data-index")))
            });
        })
    });

    function computeAgentKeyWords() {
        if (!curData || !curData.ipCounts) return;

        //agent关键字词频统计
        curData.agentKeyWords = {};
        curData.ipCounts.forEach(ipCount => {
            let agents = ipCount[1][1];
            let agentKeyWords = {};
            agents.forEach(agent => {
                let split = agent.split(/[ /,;()]/);
                split.forEach(item => {
                    if (item.length > 0 && !item.match(/[0-9._-]+/)) {
                        item = item.toLowerCase();
                        curData.agentKeyWords[item] = (curData.agentKeyWords[item] || 0) + 1;
                        agentKeyWords[item] = true;
                    }
                });
            });
            ipCount[1].push([]);
            Object.keys(agentKeyWords).forEach(keyWord => ipCount[1][2].push(keyWord));
        });

        //agent关键字按词频排序
        curData.ipCounts.forEach(ipCount => {
            ipCount[1][2] = ipCount[1][2].sort((item1, item2) => -(curData.agentKeyWords[item1] - curData.agentKeyWords[item2])).slice(0, 50);
        });
    }

    function showAgent(index) {
        if (curData && curData.ipCounts) {
            let ipData = curData.ipCounts[index];
            let title = `Agents of ${ipData[0]}`;
            let agents = `
            <ul class="list-group">
              ${ipData[1][1].sort().map(item => `<li class="list-group-item">${item}</li>`).join("\n")}
            </ul>
            `;
            showModal(title, agents);
        }
    }

    function showModal(title, body) {
        $("#myModalLabel").text(title);
        $("#myModalBody").html(body);
        $('#myModal').modal('show');
    }

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

            let tempQueue = newInstance.sendQueue;
            newInstance.sendQueue = [];
            for (let item of tempQueue) {
                sendMessage(item.msg,  item.callback);
            }
        };
        newInstance.onclose = (event) => {
            newInstance.connected = false;
        };
        newInstance.onerror = (event) => {
            newInstance.close(1000);
        };

        newInstance.sendQueue = [];
        newInstance.sendMessage = (msg, callback) => {
            if (msg !== null && msg.key && newInstance !== null) {
                msg.id = new Date().getTime() + "_" + parseInt(Math.random() * 9000 + 1000);
                if (newInstance.connected) {
                    newInstance.send(JSON.stringify(msg));
                    addListener(msg.id, callback);
                }
                else {
                    newInstance.sendQueue.push({
                        msg: msg,
                        callback: callback
                    });
                }
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