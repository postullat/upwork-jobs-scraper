require('dotenv').config();
const {sendTelegramMessage} = require("./src/service/telegramService");
const { MongoClient } = require('mongodb');
const { ProxyAgent, fetch: undisciFetch } = require('undici');
const cron = require('node-cron');

//const uri = 'mongodb://localhost:27017'; // Replace with your MongoDB URI
const uri = 'mongodb://admin:fordev123@127.0.0.1:32022/upwork?authSource=admin';


const dbName = 'upwork';
const collectionName = 'jobs';
const headers = {
'Authorization': 'Bearer oauth2v2_bd9d7d38d38c157a07f97fd060674eee',
'Content-Type': 'application/json',
'Accept': '*/*',
'Accept-Encoding': 'gzip, deflate, br',
'Accept-Language': 'en-US,en;q=0.9',
'Connection': 'keep-alive',
'User-Agent': 'PostmanRuntime/7.45.0',
'Referer': 'https://www.upwork.com/',
'Origin': 'https://www.upwork.com',
'Cookie': '__cf_bm=HkCh4UNPHNbKt8ubfyPQJFuQ8FFBN4QaTNbijoPg4pg-1754764957-1.0.1.1-kYz.isf7CAQPS39.1YXnz2Jpyd8Vp7ZSoLgIaokpKRTEn_9Q21eok34TPXP76gems1gp.0nKuIkGBx90RViK8gt8KarHz91S_wGbWK4ztCA; _cfuvid=OVx_VidaaLFtXzDb3HdPJSTSgOoluFmPaYMYye4XJd0-1754764957329-0.0.1.1-604800000; __cflb=02DiuEXPXZVk436fJfSVuuwDqLqkhavJar2gMvuMALPRM'
};

const proxyHost = 'res.proxy-seller.com';
const proxyPort = '10000';
const proxyUser = 'b02fa50863fc96e6';
const proxyPass = 'b8tRlFYa';

// Build proxy URL
const proxyUrl = `http://${proxyUser}:${proxyPass}@${proxyHost}:${proxyPort}`;
const proxyAgent = new ProxyAgent(proxyUrl);


const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function checkProxyConnection(retries = 3) {
  const delays = [5000, 10000, 15000]; // ms delays between retries

  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      console.log(`Attempt ${attempt}: Checking proxy connection...`);
      const res = await undisciFetch('https://api.ipify.org?format=json', {
        dispatcher: proxyAgent,
        signal: AbortSignal.timeout(15000)
      });

      if (!res.ok) throw new Error(`HTTP ${res.status}`);

      const data = await res.json();
      console.log(`✅ Proxy works. IP: ${data.ip}`);
      //sendTelegramMessage(`✅ Proxy works. IP: ${data.ip}`)
      return true;
    } catch (err) {
      console.error(`❌ Proxy check failed: ${err.message}`);
      sendTelegramMessage(`❌ Proxy check failed: ${err.message}`)
      if (attempt < retries) {
        const wait = delays[attempt - 1];
        console.log(`⏳ Retrying in ${wait / 1000} seconds...`);
        sendTelegramMessage(`⏳ Retrying in ${wait / 1000} seconds...`)
        await delay(wait);
      }
    }
  }
  console.error('❌ Proxy connection failed after all retries.');
  sendTelegramMessage('❌ Proxy connection failed after all retries.')
  return false;
}

async function storeJobsInMongoDB(jobs) {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log('Connected to MongoDB');
    //sendTelegramMessage('Connected to MongoDB')

    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    for (const job of jobs) {
      const filter = { id: job.id };
      const update = {
        $set: {
          id: job.id,
          title: job.title,
          description: job.description,
          jobType: job?.jobTile?.job?.jobType,
          contractorTier: job?.jobTile?.job?.contractorTier,

          hourlyEngagementType: job?.jobTile?.job?.hourlyEngagementType,
          hourlyEngagementDuration: job?.jobTile?.job?.hourlyEngagementDuration?.label,
          hourlyBudgetMin: job?.jobTile?.job?.hourlyBudgetMin ? parseFloat(job?.jobTile?.job?.hourlyBudgetMin) : null,
          hourlyBudgetMax: job?.jobTile?.job?.hourlyBudgetMax ? parseFloat(job?.jobTile?.job?.hourlyBudgetMax) : null,

          fixedPriceEngagementDuration: job?.jobTile?.job?.fixedPriceEngagementDuration?.label,
          fixedPriceAmount: job?.jobTile?.job?.fixedPriceAmount ? parseFloat(job?.jobTile?.job?.fixedPriceAmount?.amount) : null,

          skills: job?.ontologySkills?.map(skill => skill.prettyName),

          connectPrice: job.connectPrice,

          enterpriseJob: job?.jobTile?.job?.enterpriseJob,

          personsToHire: job?.jobTile?.job?.personsToHire,
          totalApplicants: job?.jobTile?.job?.totalApplicants,
          premium: job?.jobTile?.job?.premium,

          createdDateTime: job?.jobTile?.job?.createTime,
          publishedDateTime: job?.jobTile?.job?.publishTime,

          updatedAt: new Date(),
		  createdAt: new Date(),
          status: "PENDING"
		  
        }
      };
      const options = { upsert: true };
      await collection.updateOne(filter, update, options);
    }

    console.log(`Stored ${jobs.length} jobs to MongoDB (with upsert)`);
    //sendTelegramMessage(`Stored ${jobs.length} jobs to MongoDB (with upsert)`)
  } catch (err) {
    console.error('MongoDB error:', err);
    sendTelegramMessage(`MongoDB error: ${err}`)
  } finally {
    await client.close();
  }
}



async function fetchUpworkJobs() {

  const proxyOk = await checkProxyConnection();
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


  sendTelegramMessage(`--FETCH last 10 minutes jobs--`);
  try {
    while (true) {
      await delay(1000);
      console.log(`Requesting jobs with offset ${offset}`);


      const payload = structuredClone(basePayload);
      payload.variables.requestVariables.paging.offset = offset;
      payload.variables.requestVariables.paging.count = count;


      const response = await undisciFetch(url, {
        method: 'POST',
        headers,
        body: JSON.stringify(payload),
        dispatcher: proxyAgent,
        signal: AbortSignal.timeout(15000)
      });

      if (!response.ok) {
        console.error(`HTTP error: ${response.status}`);
        //sendTelegramMessage(`HTTP error: ${response.status}`)
        break;
      }

      const data = await response.json();
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
    sendTelegramMessage(`--FETCHED and stored jobs: ${allJobs.length}`)
    return allJobs;
  } catch (err) {
    console.error('Failed to fetch jobs:', err);
    sendTelegramMessage(`Failed to fetch jobs ${err}`)
  }
}


function getAvgHireRate(postedCount, totalHires) {
  if(!postedCount || !totalHires) return 0;
  if(totalHires >= postedCount) return 100;

  return totalHires * 100 / postedCount;

}

async function enrichJobsWithDetails(concurrency = 1) {

  const proxyOk = await checkProxyConnection();
  if (!proxyOk) return;

  const client = new MongoClient(uri);

  const query = `fragment JobPubOpeningInfoFragment on Job {\n    ciphertext\n    id\n    type\n    access\n    title\n    hideBudget\n    createdOn\n    notSureProjectDuration\n    notSureFreelancersToHire\n    notSureExperienceLevel\n    notSureLocationPreference\n    premium\n  }\n  fragment JobPubOpeningSegmentationDataFragment on JobSegmentation {\n    customValue\n    label\n    name\n    sortOrder\n    type\n    value\n    skill {\n      description\n      externalLink\n      prettyName\n      skill\n      id\n    }\n  }\n  fragment JobPubOpeningSandDataFragment on SandsData {\n    occupation {\n      freeText\n      ontologyId\n      prefLabel\n      id\n      uid: id\n    }\n    ontologySkills {\n      groupId\n      id\n      freeText\n      prefLabel\n      groupPrefLabel\n      relevance\n    }\n    additionalSkills {\n      groupId\n      id\n      freeText\n      prefLabel\n      relevance\n    }\n  }\n  fragment JobPubOpeningFragment on JobPubOpeningInfo {\n    status\n    postedOn\n    publishTime\n    sourcingTime\n    startDate\n    deliveryDate\n    workload\n    contractorTier\n    description\n    info {\n      ...JobPubOpeningInfoFragment\n    }\n    segmentationData {\n      ...JobPubOpeningSegmentationDataFragment\n    }\n    sandsData {\n      ...JobPubOpeningSandDataFragment\n    }\n    category {\n      name\n      urlSlug\n    }\n    categoryGroup {\n      name\n      urlSlug\n    }\n    budget {\n      amount\n      currencyCode\n    }\n    annotations {\n      tags\n    }\n    engagementDuration {\n      label\n      weeks\n    }\n    extendedBudgetInfo {\n      hourlyBudgetMin\n      hourlyBudgetMax\n      hourlyBudgetType\n    }\n    attachments @include(if: $isLoggedIn) {\n      fileName\n      length\n      uri\n    }\n    clientActivity {\n      lastBuyerActivity\n      totalApplicants\n      totalHired\n      totalInvitedToInterview\n      unansweredInvites\n      invitationsSent\n      numberOfPositionsToHire\n    }\n    deliverables\n    deadline\n    tools {\n      name\n    }\n  }\n  fragment JobQualificationsFragment on JobQualifications {\n    countries\n    earnings\n    groupRecno\n    languages\n    localDescription\n    localFlexibilityDescription\n    localMarket\n    minJobSuccessScore\n    minOdeskHours\n    onSiteType\n    prefEnglishSkill\n    regions\n    risingTalent\n    shouldHavePortfolio\n    states\n    tests\n    timezones\n    type\n    locationCheckRequired\n    group {\n      groupId\n      groupLogo\n      groupName\n    }\n    location {\n      city\n      country\n      countryTimezone\n      offsetFromUtcMillis\n      state\n      worldRegion\n    }\n    locations {\n      id\n      type\n    }\n    minHoursWeek @skip(if: $isLoggedIn)\n  }\n  fragment JobAuthDetailsOpeningFragment on JobAuthOpeningInfo {\n    job {\n      ...JobPubOpeningFragment\n    }\n    qualifications {\n      ...JobQualificationsFragment\n    }\n    questions {\n      question\n      position\n    }\n  }\n  fragment JobPubBuyerInfoFragment on JobPubBuyerInfo {\n    location {\n      offsetFromUtcMillis\n      countryTimezone\n      city\n      country\n    }\n    stats {\n      totalAssignments\n      activeAssignmentsCount\n      hoursCount\n      feedbackCount\n      score\n      totalJobsWithHires\n      totalCharges {\n        amount\n      }\n    }\n    company {\n      name @include(if: $isLoggedIn)\n      companyId @include(if: $isLoggedIn)\n      isEDCReplicated\n      contractDate\n      profile {\n        industry\n        size\n      }\n    }\n    jobs {\n      openCount\n      postedCount @include(if: $isLoggedIn)\n      openJobs {\n        id\n        uid: id\n        isPtcPrivate\n        ciphertext\n        title\n        type\n      }\n    }\n    avgHourlyJobsRate @include(if: $isLoggedIn) {\n      amount\n    }\n  }\n  fragment JobAuthDetailsBuyerWorkHistoryFragment on BuyerWorkHistoryItem {\n    isPtcJob\n    status\n    isEDCReplicated\n    isPtcPrivate\n    startDate\n    endDate\n    totalCharge\n    totalHours\n    jobInfo {\n      title\n      id\n      uid: id\n      access\n      type\n      ciphertext\n    }\n    contractorInfo {\n      contractorName\n      accessType\n      ciphertext\n    }\n    rate {\n      amount\n    }\n    feedback {\n      feedbackSuppressed\n      score\n      comment\n    }\n    feedbackToClient {\n      feedbackSuppressed\n      score\n      comment\n    }\n  }\n  fragment JobAuthDetailsBuyerFragment on JobAuthBuyerInfo {\n    enterprise\n    isPaymentMethodVerified\n    info {\n      ...JobPubBuyerInfoFragment\n    }\n    workHistory {\n      ...JobAuthDetailsBuyerWorkHistoryFragment\n    }\n  }\n  fragment JobAuthDetailsCurrentUserInfoFragment on JobCurrentUserInfo {\n    owner\n    freelancerInfo {\n      profileState\n      applied\n      devProfileCiphertext\n      hired\n      application {\n        vjApplicationId\n      }\n      pendingInvite {\n        inviteId\n      }\n      contract {\n        contractId\n        status\n      }\n      hourlyRate {\n        amount\n      }\n      qualificationsMatches {\n        matches {\n          clientPreferred\n          clientPreferredLabel\n          freelancerValue\n          freelancerValueLabel\n          qualification\n          qualified\n        }\n      }\n    }\n  }\n  query JobAuthDetailsQuery(\n    $id: ID!\n    $isFreelancerOrAgency: Boolean!\n    $isLoggedIn: Boolean!\n  ) {\n    jobAuthDetails(id: $id) {\n      hiredApplicantNames\n      opening {\n        ...JobAuthDetailsOpeningFragment\n      }\n      buyer {\n        ...JobAuthDetailsBuyerFragment\n      }\n      currentUserInfo {\n        ...JobAuthDetailsCurrentUserInfoFragment\n      }\n      similarJobs {\n        id\n        uid: id\n        ciphertext\n        title\n        snippet\n      }\n      workLocation {\n        onSiteCity\n        onSiteCountry\n        onSiteReason\n        onSiteReasonFlexible\n        onSiteState\n        onSiteType\n      }\n      phoneVerificationStatus {\n        status\n      }\n      applicantsBidsStats {\n        avgRateBid {\n          amount\n          currencyCode\n        }\n        minRateBid {\n          amount\n          currencyCode\n        }\n        maxRateBid {\n          amount\n          currencyCode\n        }\n      }\n      specializedProfileOccupationId @include(if: $isFreelancerOrAgency)\n      applicationContext @include(if: $isFreelancerOrAgency) {\n        freelancerAllowed\n        clientAllowed\n      }\n    }\n  }`;

  try {
    await client.connect();
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    // Fetch job IDs only that are not yet enriched
    const jobs = await collection
        .find({ status: "PENDING" })
        .project({ id: 1 })
        .toArray();

    console.log(`🔍 Found ${jobs.length} jobs to enrich`);
    sendTelegramMessage(`🔍 --ENRICH ${jobs.length} jobs`)
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

      try {
        const response = await undisciFetch(
            'https://www.upwork.com/api/graphql/v1?alias=gql-query-get-auth-job-details',
            {
              method: 'POST',
              headers,
              body: JSON.stringify(payload),
              dispatcher: proxyAgent,
              signal: AbortSignal.timeout(15000)

            }
        );

        if (!response.ok) {
          console.error(`❌ Failed to fetch job ${job.id}: ${response.status}`);
          sendTelegramMessage(`❌ Failed to fetch job ${job.id}: ${response.status}`)
          continue;
        }

        const data = await response.json();
        //console.log("Response data:", data);

        let jobDetails = data?.data?.jobAuthDetails;
        if (!jobDetails) {
          await collection.updateOne(
              {id: job.id},
              {
                $set: {
                  updatedAt: new Date(),
                  status: "CORRUPTED"
                },
              });
          console.warn(`⚠️ No details returned for job ${job.id}`);
          sendTelegramMessage(`⚠️ No details returned for job ${job.id}`)
          continue;
        }

        await collection.updateOne(
            {id: job.id},
            {
              $set: {
                category: jobDetails?.opening?.job?.categoryGroup?.urlSlug,
                subcategory: jobDetails?.opening?.job?.category?.urlSlug,
                totalApplicants: jobDetails?.opening?.job?.clientActivity?.totalApplicants,
                "client.location.country": jobDetails?.buyer?.info?.location?.country,
				"client.location.city": jobDetails?.buyer?.info?.location?.city,
                "client.location.countryTimezone": jobDetails?.buyer?.info?.location?.countryTimezone,

                "client.stats.industry": jobDetails?.buyer?.info?.company?.profile?.industry,
                "client.stats.companySize": jobDetails?.buyer?.info?.company?.profile?.size,

                "client.stats.avgHourlyJobsRate": jobDetails?.buyer?.info?.avgHourlyJobsRate?.amount,
                "client.stats.avgHireRate": getAvgHireRate(jobDetails?.buyer?.info?.jobs?.postedCount, jobDetails?.buyer?.info?.stats?.totalJobsWithHires),

				"client.stats.totalReviews": jobDetails?.buyer?.info?.stats?.score,
				"client.stats.totalFeedback": jobDetails?.buyer?.info?.stats?.feedbackCount,
				"client.stats.totalSpent": jobDetails?.buyer?.info?.stats?.totalCharges?.amount,
				"client.stats.totalPostedJobs": jobDetails?.buyer?.info?.jobs?.postedCount,
				"client.stats.totalHiredJobs": jobDetails?.buyer?.info?.stats?.totalJobsWithHires,
				"client.stats.hoursCount": jobDetails?.buyer?.info?.stats?.hoursCount,
				"client.stats.verificationStatus": jobDetails?.buyer?.isPaymentMethodVerified,

                questions: jobDetails?.opening?.questions ? jobDetails?.opening?.questions?.map(q => q.question) : null,
                updatedAt: new Date(),
                status: "COMPLETED"
              }
            }
        );
        totalEnriched++;
        //console.log(`✅ --Enriched job ${job.id}`);

      } catch (err) {
        console.error(`❌ Error enriching job ${job.id}: ${err.message}`);
        sendTelegramMessage(`❌ Error enriching job ${job.id}: ${err.message}`)
      }
    }




    console.log(`🎉 Enriched ${totalEnriched} of ${jobs.length} jobs`);
    sendTelegramMessage(`🎉 Enriched ${totalEnriched} of ${jobs.length} jobs`)
  } catch (err) {
    console.error('❌ MongoDB connection error:', err);
    sendTelegramMessage(`❌ MongoDB connection error ${err}`)
  } finally {
    await client.close();
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


/*fetchUpworkJobs()
    .then(() => enrichJobsWithDetails())
    .catch(err => console.error(err));*/

task.start();
console.log('Cron job with overlap prevention scheduled to run every 10 minutes');


