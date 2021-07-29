const cron = require("node-cron");
const express = require("express");
const moment = require("moment");
const fs_sync = require("fs");
const fs = fs_sync.promises;
const parse = require("csv-parse/lib/sync");
const stringify = require("csv-stringify");
const fetch = require("node-fetch");
const path = require("path");
require("dotenv").config();

const API_TOKEN = process.env.OKTA_API_TOKEN;
const baseurl = process.env.BASE_URL;

app = express();

// 일반적인 cron 스케줄 표시 매일 새벽 1시에 동작
// 테스트하려면 30 * * * * *로 교체 (매분 30초가 되면 동작)
cron.schedule("0 * * * * *", async () => {
  try {
    // Read CSV and convert to JSON to include in 'arr' array for further work
    let arr = [];
    await (async function () {
      const normalPath = path.normalize(__dirname + "/data/membersUntil.csv");
      const fileContent = await fs.readFile(
        __dirname + "/data/membersUntil.csv"
      );
      arr = parse(fileContent, { columns: true }, (err) => {
        if (err) {
          console.log("Error in reading .csv file!");
        }
      });
    })();

    // 멤버십 제외 작업 처리된 사용자
    const completed = [];

    // 아직 작업 처리되지 않은 사용자
    const remaining = [];

    // 오늘 날짜 저장
    let now = moment();

    // API Rate Limit에 걸리지 않도록 각 사용자 작업 사이에 3000ms (3초) 간격 발생
    // 한 사용자 당 3 call이므로 최대 분당 60 call로 제한
    let idx = 0;
    let i = setInterval(async () => {
      let currentUser = arr[idx];
      // to handle a library bug not catching the first key of the parsed object
      let userNameKey = Object.keys(arr[idx])[0];
      let userName = arr[idx][userNameKey];
      let groupName = arr[idx].groupName;
      if (idx === 0) {
        // 전처리 내용
        console.log(
          "Starting the daily automation process: " + now.format("YYYY-MM-DD")
        );
      }
      const normalizedMemberUntil = moment(new Date(currentUser.memberUntil));
      console.log(normalizedMemberUntil);
      // 권한 기간 만료 판단
      const isExpired = moment(normalizedMemberUntil).isBefore(now, "day");
      console.log(isExpired);
      // 권한 기간이 만료된 경우, 해당 유저의 userId, 그룹의 groupId 확인 후 유저를 그룹에서 삭제
      if (isExpired) {
        // 리퀘스트 헤더 세팅
        let headers = {
          "Content-Type": "application/json",
          Authorization: `SSWS ${API_TOKEN}`,
        };
        // GET userId
        let userResponse = await fetch(baseurl + `/api/v1/users/${userName}`, {
          method: "get",
          headers,
        });
        let userJson = await userResponse.json();
        let userId = userJson.id;
        // console.log(userName, userId);

        // GET GroupId
        let groupResponse = await fetch(
          baseurl + `/api/v1/groups?q=${groupName}&limit=1`,
          {
            method: "get",
            headers,
          }
        );
        let groupJson = await groupResponse.json();
        if (groupJson.length === 0) {
          throw new Error("Cannot find the group with the name: " + groupName);
        }
        let groupId = groupJson[0].id;
        // console.log(groupJson[0].profile.name, groupId);

        //Remove the user from the group
        let removeUserResponse = await fetch(
          baseurl + `/api/v1/groups/${groupId}/users/${userId}`,
          {
            method: "delete",
            headers,
          }
        );
        // console.log(
        //   removeUserResponse.status + " " + removeUserResponse.statusText
        // );
        completed.push(currentUser);
        console.log(
          userName +
            " is removed from " +
            currentUser.groupName +
            ". The user is logged in ./logs/membersExcluded-" +
            now.format("YYYY-MM-DD") +
            ".csv"
        );
      } else {
        // put this user to remaining array
        // log the result to console
        remaining.push(currentUser);
        console.log(
          userName +
            " awaits the next automation. The user will remain in membersUntil.csv"
        );
      }
      idx += 1;
      if (idx === arr.length) {
        // 마무리 처리
        // TODO: overwrite schedule.csv (use remaining array to convert JSON to CSV)
        if (completed.length !== 0) {
          // 그룹에서 빠져나간 사용자가 있을 경우에는, 일일 로그 파일 생성
          const filePath_raw =
            "./logs/membersExcluded-" + now.format("YYYY-MM-DD") + ".csv";
          const filePath = path.normalize(filePath_raw);
          fs_sync.closeSync(fs_sync.openSync(filePath, "w"));
        }

        stringify(
          remaining,
          {
            header: true,
          },
          function (err, output) {
            if (err) {
              console.log("remaining members overwrite error");
            }
            const normalPath = path.normalize(
              __dirname + "/data/membersUntil.csv"
            );
            fs.writeFile(normalPath, output);
          }
        );

        stringify(
          completed,
          {
            header: true,
          },
          function (err, output) {
            if (err) {
              console.log("completed file write error");
            }
            const normalPath = path.normalize(
              __dirname +
                "/logs/membersExcluded-" +
                now.format("YYYY-MM-DD") +
                ".csv"
            );
            fs.writeFile(normalPath, output);
          }
        );
        console.log(
          "Closing the daily automation process: " + now.format("YYYY-MM-DD")
        );

        clearInterval(i);
      }
    }, 3000);
  } catch (err) {
    console.log(err);
  }
});

app.listen(3000, () => {
  console.log("Node cronjob server is running on http://localhost:3000");
});
