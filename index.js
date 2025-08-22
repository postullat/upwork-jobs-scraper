import 'dotenv/config';
import { sendTelegramMessage } from './src/service/telegramService.js';
import { ProxyAgent, fetch as undisciFetch } from 'undici';
import cron from 'node-cron';
import {getOAuth2v2Cookies} from "./src/pupeteer-cookies.js";
import {enrichJob, getPendingJobs, setJobStatus, storeJobsInMongoDB} from "./src/dao/upworkJobsDao.js";
import {checkProxyConnection} from "./src/proxy/checkProxy.js";

const accessTokenInitial = process.env.ACCESS_TOKEN_INITIAL;


const headers = {
'Authorization': `Bearer ${accessTokenInitial}`,
'Content-Type': 'application/json',
'Accept': '*/*',
'Accept-Encoding': 'gzip, deflate, br',
'Accept-Language': 'en-US,en;q=0.9',
'Connection': 'keep-alive',
'User-Agent': 'PostmanRuntime/7.45.0',
'Referer': 'https://www.upwork.com/',
'Origin': 'https://www.upwork.com'
};

const proxyHost = process.env.PROXY_HOST;
const proxyPort = process.env.PROXY_PORT;
const proxyUser = process.env.PROXY_USER;
const proxyPass = process.env.PROXY_PASS;

// Build proxy URL
const proxyUrl = `http://${proxyUser}:${proxyPass}@${proxyHost}:${proxyPort}`;
const proxyAgent = new ProxyAgent(proxyUrl);

console.log(`proxyHost: ${proxyHost}`);
console.log(`proxyPort: ${proxyPort}`);
console.log(`proxyUser: ${proxyUser}`);
console.log(`proxyPass: ${proxyPass}`);
console.log(`Bearer: ${accessTokenInitial}`);

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));


async function fetchUpworkJobs() {

  const proxyOk = await checkProxyConnection(proxyAgent);
  if (!proxyOk) return;

  const url = 'https://www.upwork.com/api/graphql/v1?alias=userJobSearch';

  const basePayload = {
    "query": "\n  query UserJobSearch($requestVariables: UserJobSearchV1Request!) {\n    search {\n      universalSearchNuxt {\n        userJobSearchV1(request: $requestVariables) {\n          paging {\n            total\n            offset\n            count\n          }\n          \n    facets {\n      jobType \n    {\n      key\n      value\n    }\n  \n      workload \n    {\n      key\n      value\n    }\n  \n      clientHires \n    {\n      key\n      value\n    }\n  \n      durationV3 \n    {\n      key\n      value\n    }\n  \n      amount \n    {\n      key\n      value\n    }\n  \n      contractorTier \n    {\n      key\n      value\n    }\n  \n      contractToHire \n    {\n      key\n      value\n    }\n  \n      \n    paymentVerified: payment \n    {\n      key\n      value\n    }\n  \n    proposals \n    {\n      key\n      value\n    }\n  \n    previousClients \n    {\n      key\n      value\n    }\n  \n  \n    }\n  \n          results {\n            id\n            title\n            description\n            relevanceEncoded\n            ontologySkills {\n              uid\n              parentSkillUid\n              prefLabel\n              prettyName: prefLabel\n              freeText\n              highlighted\n            }\n            \n    isSTSVectorSearchResult\n    connectPrice\n    applied\n    upworkHistoryData {\n      client {\n        paymentVerificationStatus\n        country\n        totalReviews\n        totalFeedback\n        hasFinancialPrivacy\n        totalSpent {\n          isoCurrencyCode\n          amount\n        }\n      }\n      freelancerClientRelation {\n        lastContractRid\n        companyName\n        lastContractTitle\n      }\n    }\n            jobTile {\n              job {\n                id\n                ciphertext: cipherText\n                jobType\n                weeklyRetainerBudget\n                hourlyBudgetMax\n                hourlyBudgetMin\n                hourlyEngagementType\n                contractorTier\n                sourcingTimestamp\n                createTime\n                publishTime\n                \n    enterpriseJob\n    personsToHire\n    premium\n    totalApplicants\n  \n                hourlyEngagementDuration {\n                  rid\n                  label\n                  weeks\n                  mtime\n                  ctime\n                }\n                fixedPriceAmount {\n                  isoCurrencyCode\n                  amount\n                }\n                fixedPriceEngagementDuration {\n                  id\n                  rid\n                  label\n                  weeks\n                  ctime\n                  mtime\n                }\n              }\n            }\n          }\n        }\n      }\n    }\n  }\n",
    "variables": {
      "requestVariables": {
        "userQuery": "",
        "sort": "recency",
        "highlight": true,
        "paging": {
          "offset": 0,
          "count": 50
        }
      }
    }
  };
  const count = 50;
  let offset = 0;
  let allJobs = [];

  const tenMinutesAgo = Date.now() - 10 * 60 * 1000;

  const isRecent = (publishTime) => {
  try {
    const publishTimestamp = new Date(publishTime).getTime();
    return publishTimestamp > tenMinutesAgo;
  } catch (e) {
    console.warn('⚠️ Failed to parse publishTime:', publishTime);
    sendTelegramMessage(`⚠️ Failed to parse publishTime: ${publishTime}`)
    return false;
  }
};


  await sendTelegramMessage(`--FETCH last 10 minutes jobs--`);
  try {
    while (true) {
      await delay(1000);
      console.log(`Requesting jobs with offset ${offset}`);


      const payload = structuredClone(basePayload);
      payload.variables.requestVariables.paging.offset = offset;
      payload.variables.requestVariables.paging.count = count;

      let reqPayload = {
        method: 'POST',
        headers,
        body: JSON.stringify(payload),
        dispatcher: proxyAgent,
        signal: AbortSignal.timeout(50000)
      };
      let response = await undisciFetch(url, reqPayload);
      let json;
      if (!response.ok) {

        let apiAccessResolved = false;
        if (response.status === 401) {
          const cookies = await getOAuth2v2Cookies();
          for (const cookie of cookies) {
            headers['Authorization'] = `Bearer ${cookie.value}`;
            reqPayload.headers = headers;
            response = await undisciFetch(url, reqPayload);
            if (response.ok) {
              json = await response.json();
              if(json?.data) {
                  apiAccessResolved = true;
                  break;
              }
            }
          }
        } else {
            break;
        }

        if(!apiAccessResolved) {
            console.error(`HTTP error: ${response.status}`);
            await sendTelegramMessage(`HTTP error: ${response.status}. Can not resolve access token`);
            break;
        }
      }

      const data = json ? json : await response.json();
      json = undefined;
      //console.log("Response data:",data);
      const results = data?.data?.search?.universalSearchNuxt?.userJobSearchV1?.results;

      if (!results || results.length === 0) {
        console.log('No more results');
        //sendTelegramMessage('No more results')
        break;
      }

      const recentJobs = results.filter(job => {
        const publishTime = job?.jobTile?.job?.publishTime;
        return publishTime && isRecent(publishTime);
      });

      if (recentJobs.length > 0) {
        await storeJobsInMongoDB(recentJobs);
        allJobs.push(...recentJobs);
        console.log(`Stored ${recentJobs.length} recent jobs.`);
        //sendTelegramMessage(`Stored ${recentJobs.length} recent jobs.`)
      }

      // Stop if ALL jobs in the current page are older than 10 mins
      const allOld = results.every(job => {
        const publishTime = job?.jobTile?.job?.publishTime;
        return !publishTime || !isRecent(publishTime);
      });

      if (allOld) {
        console.log('All jobs in this page are older than 10 minutes. Stopping.');
        //sendTelegramMessage('All jobs in this page are older than 10 minutes. Stopping.')
        break;
      }

      offset += count;
    }

    console.log(`--FETCHED and stored jobs: ${allJobs.length}`);
    await sendTelegramMessage(`--FETCHED and stored jobs: ${allJobs.length}`)
    return allJobs;
  } catch (err) {
    console.error('Failed to fetch jobs:', err);
    await sendTelegramMessage(`Failed to fetch jobs ${err}`)
  }
}


async function enrichJobsWithDetails(concurrency = 1) {

  const proxyOk = await checkProxyConnection(proxyAgent);
  if (!proxyOk) return;


  const url = 'https://www.upwork.com/api/graphql/v1?alias=gql-query-get-auth-job-details';
  const query = `fragment JobPubOpeningInfoFragment on Job {\n    ciphertext\n    id\n    type\n    access\n    title\n    hideBudget\n    createdOn\n    notSureProjectDuration\n    notSureFreelancersToHire\n    notSureExperienceLevel\n    notSureLocationPreference\n    premium\n  }\n  fragment JobPubOpeningSegmentationDataFragment on JobSegmentation {\n    customValue\n    label\n    name\n    sortOrder\n    type\n    value\n    skill {\n      description\n      externalLink\n      prettyName\n      skill\n      id\n    }\n  }\n  fragment JobPubOpeningSandDataFragment on SandsData {\n    occupation {\n      freeText\n      ontologyId\n      prefLabel\n      id\n      uid: id\n    }\n    ontologySkills {\n      groupId\n      id\n      freeText\n      prefLabel\n      groupPrefLabel\n      relevance\n    }\n    additionalSkills {\n      groupId\n      id\n      freeText\n      prefLabel\n      relevance\n    }\n  }\n  fragment JobPubOpeningFragment on JobPubOpeningInfo {\n    status\n    postedOn\n    publishTime\n    sourcingTime\n    startDate\n    deliveryDate\n    workload\n    contractorTier\n    description\n    info {\n      ...JobPubOpeningInfoFragment\n    }\n    segmentationData {\n      ...JobPubOpeningSegmentationDataFragment\n    }\n    sandsData {\n      ...JobPubOpeningSandDataFragment\n    }\n    category {\n      name\n      urlSlug\n    }\n    categoryGroup {\n      name\n      urlSlug\n    }\n    budget {\n      amount\n      currencyCode\n    }\n    annotations {\n      tags\n    }\n    engagementDuration {\n      label\n      weeks\n    }\n    extendedBudgetInfo {\n      hourlyBudgetMin\n      hourlyBudgetMax\n      hourlyBudgetType\n    }\n    attachments @include(if: $isLoggedIn) {\n      fileName\n      length\n      uri\n    }\n    clientActivity {\n      lastBuyerActivity\n      totalApplicants\n      totalHired\n      totalInvitedToInterview\n      unansweredInvites\n      invitationsSent\n      numberOfPositionsToHire\n    }\n    deliverables\n    deadline\n    tools {\n      name\n    }\n  }\n  fragment JobQualificationsFragment on JobQualifications {\n    countries\n    earnings\n    groupRecno\n    languages\n    localDescription\n    localFlexibilityDescription\n    localMarket\n    minJobSuccessScore\n    minOdeskHours\n    onSiteType\n    prefEnglishSkill\n    regions\n    risingTalent\n    shouldHavePortfolio\n    states\n    tests\n    timezones\n    type\n    locationCheckRequired\n    group {\n      groupId\n      groupLogo\n      groupName\n    }\n    location {\n      city\n      country\n      countryTimezone\n      offsetFromUtcMillis\n      state\n      worldRegion\n    }\n    locations {\n      id\n      type\n    }\n    minHoursWeek @skip(if: $isLoggedIn)\n  }\n  fragment JobAuthDetailsOpeningFragment on JobAuthOpeningInfo {\n    job {\n      ...JobPubOpeningFragment\n    }\n    qualifications {\n      ...JobQualificationsFragment\n    }\n    questions {\n      question\n      position\n    }\n  }\n  fragment JobPubBuyerInfoFragment on JobPubBuyerInfo {\n    location {\n      offsetFromUtcMillis\n      countryTimezone\n      city\n      country\n    }\n    stats {\n      totalAssignments\n      activeAssignmentsCount\n      hoursCount\n      feedbackCount\n      score\n      totalJobsWithHires\n      totalCharges {\n        amount\n      }\n    }\n    company {\n      name @include(if: $isLoggedIn)\n      companyId @include(if: $isLoggedIn)\n      isEDCReplicated\n      contractDate\n      profile {\n        industry\n        size\n      }\n    }\n    jobs {\n      openCount\n      postedCount @include(if: $isLoggedIn)\n      openJobs {\n        id\n        uid: id\n        isPtcPrivate\n        ciphertext\n        title\n        type\n      }\n    }\n    avgHourlyJobsRate @include(if: $isLoggedIn) {\n      amount\n    }\n  }\n  fragment JobAuthDetailsBuyerWorkHistoryFragment on BuyerWorkHistoryItem {\n    isPtcJob\n    status\n    isEDCReplicated\n    isPtcPrivate\n    startDate\n    endDate\n    totalCharge\n    totalHours\n    jobInfo {\n      title\n      id\n      uid: id\n      access\n      type\n      ciphertext\n    }\n    contractorInfo {\n      contractorName\n      accessType\n      ciphertext\n    }\n    rate {\n      amount\n    }\n    feedback {\n      feedbackSuppressed\n      score\n      comment\n    }\n    feedbackToClient {\n      feedbackSuppressed\n      score\n      comment\n    }\n  }\n  fragment JobAuthDetailsBuyerFragment on JobAuthBuyerInfo {\n    enterprise\n    isPaymentMethodVerified\n    info {\n      ...JobPubBuyerInfoFragment\n    }\n    workHistory {\n      ...JobAuthDetailsBuyerWorkHistoryFragment\n    }\n  }\n  fragment JobAuthDetailsCurrentUserInfoFragment on JobCurrentUserInfo {\n    owner\n    freelancerInfo {\n      profileState\n      applied\n      devProfileCiphertext\n      hired\n      application {\n        vjApplicationId\n      }\n      pendingInvite {\n        inviteId\n      }\n      contract {\n        contractId\n        status\n      }\n      hourlyRate {\n        amount\n      }\n      qualificationsMatches {\n        matches {\n          clientPreferred\n          clientPreferredLabel\n          freelancerValue\n          freelancerValueLabel\n          qualification\n          qualified\n        }\n      }\n    }\n  }\n  query JobAuthDetailsQuery(\n    $id: ID!\n    $isFreelancerOrAgency: Boolean!\n    $isLoggedIn: Boolean!\n  ) {\n    jobAuthDetails(id: $id) {\n      hiredApplicantNames\n      opening {\n        ...JobAuthDetailsOpeningFragment\n      }\n      buyer {\n        ...JobAuthDetailsBuyerFragment\n      }\n      currentUserInfo {\n        ...JobAuthDetailsCurrentUserInfoFragment\n      }\n      similarJobs {\n        id\n        uid: id\n        ciphertext\n        title\n        snippet\n      }\n      workLocation {\n        onSiteCity\n        onSiteCountry\n        onSiteReason\n        onSiteReasonFlexible\n        onSiteState\n        onSiteType\n      }\n      phoneVerificationStatus {\n        status\n      }\n      applicantsBidsStats {\n        avgRateBid {\n          amount\n          currencyCode\n        }\n        minRateBid {\n          amount\n          currencyCode\n        }\n        maxRateBid {\n          amount\n          currencyCode\n        }\n      }\n      specializedProfileOccupationId @include(if: $isFreelancerOrAgency)\n      applicationContext @include(if: $isFreelancerOrAgency) {\n        freelancerAllowed\n        clientAllowed\n      }\n    }\n  }`;

  try {


    // Fetch job IDs only that are not yet enriched
    const jobs = await getPendingJobs();

    console.log(`🔍 Found ${jobs.length} jobs to enrich`);
    await sendTelegramMessage(`🔍 --ENRICH ${jobs.length} jobs`)
    let totalEnriched = 0;
    for(let job of jobs) {
      await delay(2800);
      const payload = {
        query,
        variables: {
          id: `~02${job.id}`,
          isLoggedIn: true,
          isFreelancerOrAgency: true,
        }
      };

      let reqPayload = {
          method: 'POST',
          headers,
          body: JSON.stringify(payload),
          dispatcher: proxyAgent,
          signal: AbortSignal.timeout(25000)

      }
      try {
        let response = await undisciFetch(url, reqPayload);
        let json;

        if (!response.ok) {

          let apiAccessResolved = false;
          if (response.status === 401) {
              const cookies = await getOAuth2v2Cookies();
              for (const cookie of cookies) {
                  headers['Authorization'] = `Bearer ${cookie.value}`;
                  reqPayload.headers = headers;
                  response = await undisciFetch(url, reqPayload);
                  if (response.ok) {
                      json = await response.json();
                      if(json?.data) {
                          apiAccessResolved = true;
                          break;
                      }
                  }
              }
          } else {
              console.error(`❌ Failed to fetch job ${job.id}: ${response.status}`);
              await sendTelegramMessage(`❌ Failed to fetch job ${job.id}: ${response.status}`)
              continue;
          }

          if(!apiAccessResolved) {
              console.error(`❌ Failed to fetch job ${job.id}: ${response.status}. Can not resolve access token`);
              await sendTelegramMessage(`❌ Failed to fetch job ${job.id}: ${response.status}. Can not resolve access token`)
              break;
          }

        }

        const data = json ? json : await response.json();
        json = undefined;
        //console.log("Response data:", data);

        let jobDetails = data?.data?.jobAuthDetails;
        if (!jobDetails) {
          await setJobStatus(job.id, "CORRUPTED");
          console.warn(`⚠️ No details returned for job ${job.id}`);
          await sendTelegramMessage(`⚠️ No details returned for job ${job.id}`)
          continue;
        }

        await enrichJob(job, jobDetails);

        totalEnriched++;

      } catch (err) {
        console.error(`❌ Error enriching job ${job.id}: ${err.message}`);
        await sendTelegramMessage(`❌ Error enriching job ${job.id}: ${err.message}`)
      }
    }

    console.log(`🎉 Enriched ${totalEnriched} of ${jobs.length} jobs`);
    sendTelegramMessage(`🎉 Enriched ${totalEnriched} of ${jobs.length} jobs`)
  } catch (err) {
    console.error('❌ MongoDB connection error:', err);
    sendTelegramMessage(`❌ MongoDB connection error ${err}`)
  }
}



let isJobRunning = false;

const task = cron.schedule('*/10 * * * *', async () => {
  if (isJobRunning) {
    console.log(`[${new Date().toISOString()}] Job already/still running, skipping...`);
    sendTelegramMessage(`[${new Date().toISOString()}] Job already/still running, skipping...`)
    return;
  }

  isJobRunning = true;
  console.log(`[${new Date().toISOString()}] Starting scheduled job...`);
  //sendTelegramMessage(`[${new Date().toISOString()}] Starting scheduled job...`)

  try {
    await fetchUpworkJobs();
    await enrichJobsWithDetails();
    console.log(`[${new Date().toISOString()}] Job completed successfully`);
    //sendTelegramMessage(`[${new Date().toISOString()}] Job completed successfully`)
  } catch (err) {
    console.error(`[${new Date().toISOString()}] Job failed:`, err.message);
    sendTelegramMessage(`[${new Date().toISOString()}] Job failed: ${err.message}`)
  } finally {
    isJobRunning = false;
  }
}, {
  scheduled: true,
  timezone: "UTC"
});


fetchUpworkJobs()
    .then(() => enrichJobsWithDetails())
    .catch(err => console.error(err));

//task.start();
console.log('Cron job with overlap prevention scheduled to run every 10 minutes');


