const { MongoClient } = require('mongodb');

const uri = 'mongodb://localhost:27017'; // Replace with your MongoDB URI
const dbName = 'upworkDB';
const collectionName = 'jobs';
const headers = {
'Authorization': 'Bearer oauth2v2_cd755b74acf57a016a4cab17ecf10411',
'Content-Type': 'application/json',
'Accept': '*/*',
'Accept-Encoding': 'gzip, deflate, br',
'Accept-Language': 'en-US,en;q=0.9',
'Connection': 'keep-alive',
'User-Agent': 'PostmanRuntime/7.45.0',
'Referer': 'https://www.upwork.com/',
'Origin': 'https://www.upwork.com',
'Cookie': '__cf_bm=K79LiDYk43..DwXGNR1ksav6z0FCyBxIP0RAWz5HmM0-1754591158-1.0.1.1-jwr8hComhig8YQ01EYXWqbnXO8qsQLzcUhFiVQNeDU8.1AQ2xObE3wez78RdnusBBnvAvfvcacUbUwmDokflzENRw4WLziQ0RGZ4tHAvZ5U; _cfuvid=xWLdxRTOhZH2GpR2oqmKETAp8kO9AHC3BCtYzWZf6FM-1754591158173-0.0.1.1-604800000; AWSALBTG=eBkxT4Lv5c8vg8OvX/r5qTzZD5lWLa1Gm0+2Ry+9DALGpY7w1yujrzs3BtSGdw3aUUlK2Eds+tG9JhI+ea9PVnSNZAKAItspC13LJCBOK4xbq3hO7WFXHPovVCsLq+BuonBGCSOiMa1YvGJJFPp8jz4hf3SGLjxtrBM3SkMoV7cE; AWSALBTGCORS=eBkxT4Lv5c8vg8OvX/r5qTzZD5lWLa1Gm0+2Ry+9DALGpY7w1yujrzs3BtSGdw3aUUlK2Eds+tG9JhI+ea9PVnSNZAKAItspC13LJCBOK4xbq3hO7WFXHPovVCsLq+BuonBGCSOiMa1YvGJJFPp8jz4hf3SGLjxtrBM3SkMoV7cE; __cflb=02DiuEXPXZVk436fJfSVuuwDqLqkhavJbn7FS9kbLwEfM'
};

async function storeJobsInMongoDB(jobs) {
  const client = new MongoClient(uri, { useUnifiedTopology: true });

  try {
    await client.connect();
    console.log('Connected to MongoDB');

    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    for (const job of jobs) {
      const filter = { id: job.id };
      const update = {
        $set: {
          ...job,
          updatedAt: new Date()
        }
      };
      const options = { upsert: true };
      await collection.updateOne(filter, update, options);
    }

    console.log(`Stored ${jobs.length} jobs to MongoDB (with upsert)`);
  } catch (err) {
    console.error('MongoDB error:', err);
  } finally {
    await client.close();
  }
}



async function fetchUpworkJobs() {
  const url = 'https://www.upwork.com/api/graphql/v1?alias=visitorJobSearch';

  const basePayload = {
    "query": "\n  query VisitorJobSearch($requestVariables: VisitorJobSearchV1Request!) {\n    search {\n      universalSearchNuxt {\n        visitorJobSearchV1(request: $requestVariables) {\n          paging {\n            total\n            offset\n            count\n          }\n          \n    facets {\n      jobType \n    {\n      key\n      value\n    }\n  \n      workload \n    {\n      key\n      value\n    }\n  \n      clientHires \n    {\n      key\n      value\n    }\n  \n      durationV3 \n    {\n      key\n      value\n    }\n  \n      amount \n    {\n      key\n      value\n    }\n  \n      contractorTier \n    {\n      key\n      value\n    }\n  \n      contractToHire \n    {\n      key\n      value\n    }\n  \n      \n    }\n  \n          results {\n            id\n            title\n            description\n            relevanceEncoded\n            ontologySkills {\n              uid\n              parentSkillUid\n              prefLabel\n              prettyName: prefLabel\n              freeText\n              highlighted\n            }\n            \n            jobTile {\n              job {\n                id\n         ciphertext: cipherText\n                jobType\n                weeklyRetainerBudget\n                hourlyBudgetMax\n                hourlyBudgetMin\n                hourlyEngagementType\n                contractorTier\n                sourcingTimestamp\n                createTime\n                publishTime\n                \n                hourlyEngagementDuration {\n                  rid\n                  label\n                  weeks\n                  mtime\n                  ctime\n                }\n                fixedPriceAmount {\n                  isoCurrencyCode\n                  amount\n                }\n                fixedPriceEngagementDuration {\n                  id\n                  rid\n                  label\n                  weeks\n                  ctime\n                  mtime\n                }\n              }\n            }\n          }\n        }\n      }\n    }\n  }\n  ",
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

  let offset = 0;
  const count = 50;
  let totalResults = 0;
  let allJobs = [];

  try {
    // First request to get total count
    let payload = JSON.parse(JSON.stringify(basePayload)); // Deep copy
    payload.variables.requestVariables.paging.offset = offset;
    payload.variables.requestVariables.paging.count = count;

    console.log(`Making initial request with offset: ${offset}, count: ${count}`);

    const response = await fetch(url, {
      method: 'POST',
      headers: headers,
      body: JSON.stringify(payload)
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const data = await response.json();
    console.log('First response:', JSON.stringify(data, null, 2));

    // Extract pagination info
    const searchResults = data?.data?.search?.universalSearchNuxt?.visitorJobSearchV1;
    if (!searchResults) {
      console.error('Unexpected response structure');
      return;
    }

    totalResults = searchResults.paging.total;
    console.log(`Total results available: ${totalResults}`);

	if (searchResults.results) {
      await storeJobsInMongoDB(searchResults.results);
    }

    // Add first batch of jobs
    if (searchResults.results) {
      allJobs.push(...searchResults.results);
      console.log(`Retrieved ${searchResults.results.length} jobs from first request`);
    }

    // Continue fetching remaining pages
    offset += count;

    while (offset < totalResults) {
      console.log(`\nMaking request with offset: ${offset}, count: ${count}`);

      // Update payload for next request
      payload = JSON.parse(JSON.stringify(basePayload)); // Fresh deep copy
      payload.variables.requestVariables.paging.offset = offset;
      payload.variables.requestVariables.paging.count = count;

      const nextResponse = await fetch(url, {
        method: 'POST',
        headers: headers,
        body: JSON.stringify(payload)
      });

      if (!nextResponse.ok) {
        console.error(`HTTP error on offset ${offset}! status: ${nextResponse.status}`);
        break;
      }

      const nextData = await nextResponse.json();
      console.log(`Response for offset ${offset}:`, JSON.stringify(nextData, null, 2));

      const nextSearchResults = nextData?.data?.search?.universalSearchNuxt?.visitorJobSearchV1;
      if (nextSearchResults?.results) {
		await storeJobsInMongoDB(nextSearchResults.results);
        //allJobs.push(...nextSearchResults.results);
        console.log(`Retrieved ${nextSearchResults.results.length} jobs from offset ${offset}`);
      } else {
        console.log('No more results found');
        break;
      }

      offset += count;

      // Add small delay to be respectful to the API
      await new Promise(resolve => setTimeout(resolve, 1000));
    }

    console.log(`\nFinal Summary:`);
    console.log(`Total jobs retrieved: ${allJobs.length}`);
    console.log(`Total jobs available: ${totalResults}`);

    return allJobs;

  } catch (error) {
    console.error('Error fetching jobs:', error);
  }
}

async function enrichJobsWithDetails(concurrency = 2) {
  const client = new MongoClient(uri, { useUnifiedTopology: true });

  const query = `fragment JobPubOpeningInfoFragment on Job {
    ciphertext id type access title hideBudget createdOn notSureProjectDuration
    notSureFreelancersToHire notSureExperienceLevel notSureLocationPreference premium
  }
  fragment JobPubOpeningSegmentationDataFragment on JobSegmentation {
    customValue label name sortOrder type value skill {
      description externalLink prettyName skill id
    }
  }
  fragment JobPubOpeningSandDataFragment on SandsData {
    occupation {
      freeText ontologyId prefLabel id uid: id
    }
    ontologySkills {
      groupId id freeText prefLabel groupPrefLabel relevance
    }
    additionalSkills {
      groupId id freeText prefLabel relevance
    }
  }
  fragment JobPubOpeningFragment on JobPubOpeningInfo {
    status postedOn publishTime sourcingTime startDate deliveryDate workload contractorTier
    description info {
      ...JobPubOpeningInfoFragment
    }
    segmentationData {
      ...JobPubOpeningSegmentationDataFragment
    }
    sandsData {
      ...JobPubOpeningSandDataFragment
    }
    category {
      name urlSlug
    }
    categoryGroup {
      name urlSlug
    }
    budget {
      amount currencyCode
    }
    annotations {
      tags
    }
    engagementDuration {
      label weeks
    }
    extendedBudgetInfo {
      hourlyBudgetMin hourlyBudgetMax hourlyBudgetType
    }
    attachments @include(if: $isLoggedIn) {
      fileName length uri
    }
    clientActivity {
      lastBuyerActivity totalApplicants totalHired totalInvitedToInterview unansweredInvites
      invitationsSent numberOfPositionsToHire
    }
    deliverables deadline tools {
      name
    }
  }
  fragment JobPubBuyerInfoFragment on JobPubBuyerInfo {
    location {
      offsetFromUtcMillis countryTimezone city country
    }
    stats {
      totalAssignments activeAssignmentsCount hoursCount feedbackCount score
      totalJobsWithHires totalCharges {
        amount
      }
    }
    company {
      name @include(if: $isLoggedIn) companyId @include(if: $isLoggedIn)
      isEDCReplicated contractDate profile {
        industry size
      }
    }
    jobs {
      openCount postedCount @include(if: $isLoggedIn)
      openJobs {
        id uid: id isPtcPrivate ciphertext title type
      }
    }
    avgHourlyJobsRate @include(if: $isLoggedIn) {
      amount
    }
  }
  fragment JobQualificationsFragment on JobQualifications {
    countries earnings groupRecno languages localDescription localFlexibilityDescription
    localMarket minJobSuccessScore minOdeskHours onSiteType prefEnglishSkill regions
    risingTalent shouldHavePortfolio states tests timezones type locationCheckRequired
    group {
      groupId groupLogo groupName
    }
    location {
      city country countryTimezone offsetFromUtcMillis state worldRegion
    }
    locations {
      id type
    }
    minHoursWeek @skip(if: $isLoggedIn)
  }
  fragment JobPubSimilarJobsFragment on PubSimilarJob {
    id ciphertext title description engagement durationLabel contractorTier type createdOn renewedOn
    amount {
      amount
    }
    maxAmount {
      amount
    }
    ontologySkills {
      id prefLabel
    }
    hourlyBudgetMin hourlyBudgetMax
  }
  query JobPubDetailsQuery($id: ID!, $isLoggedIn: Boolean!) {
    jobPubDetails(id: $id) {
      opening {
        ...JobPubOpeningFragment
      }
      qualifications {
        ...JobQualificationsFragment
      }
      buyer {
        ...JobPubBuyerInfoFragment
      }
      similarJobs {
        ...JobPubSimilarJobsFragment
      }
      buyerExtra {
        isPaymentMethodVerified
      }
    }
  }`;

  try {
    await client.connect();
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    // Fetch job IDs only that are not yet enriched
    const jobs = await collection.find({ detailedInfo: { $exists: false } }).project({ id: 1 }).toArray();

    console.log(`🔍 Found ${jobs.length} jobs to enrich`);

    // Create task runner function for each job
    const createEnrichmentTask = (jobId) => async () => {
      const payload = {
        query,
        variables: {
          id: `~02${jobId}`,
          isLoggedIn: false
        }
      };

      try {
        const response = await fetch(
          'https://www.upwork.com/api/graphql/v1?alias=gql-query-get-visitor-job-details',
          {
            method: 'POST',
            headers,
            body: JSON.stringify(payload)
          }
        );

        if (!response.ok) {
          console.error(`❌ Failed to fetch job ${jobId}: ${response.status}`);
          return;
        }

        const data = await response.json();
		console.log("Response data:", data);

        if (!data?.data?.jobPubDetails) {
          console.warn(`⚠️ No details returned for job ${jobId}`);
          return;
        }

        await collection.updateOne(
          { id: jobId },
          {
            $set: {
              detailedInfo: data.data.jobPubDetails,
              detailsFetchedAt: new Date()
            }
          }
        );

        console.log(`✅ Enriched job ${jobId}`);
      } catch (err) {
        console.error(`❌ Error enriching job ${jobId}: ${err.message}`);
      }
    };

    // Batch execution with limited concurrency
    const runInBatches = async (tasks, concurrency) => {
      const queue = [...tasks];
      let active = [];

      while (queue.length > 0 || active.length > 0) {
        while (active.length < concurrency && queue.length > 0) {
          const task = queue.shift();
          const promise = task().finally(() => {
            active = active.filter(p => p !== promise);
          });
          active.push(promise);
        }

        // Wait for at least one task to finish
        if (active.length > 0) {
          await Promise.race(active);
        }
      }
    };

    const tasks = jobs.map(job => createEnrichmentTask(job.id));
    await runInBatches(tasks, concurrency);

    console.log(`🎉 All jobs enriched`);
  } catch (err) {
    console.error('❌ MongoDB connection error:', err);
  } finally {
    await client.close();
  }
}


// Call the function
fetchUpworkJobs();
//enrichJobsWithDetails();
