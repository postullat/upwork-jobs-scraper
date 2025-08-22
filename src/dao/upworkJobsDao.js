import {MongoClient} from "mongodb";
import {sendTelegramMessage} from "../service/telegramService.js";

//const uri = 'mongodb://localhost:27017'; // Replace with your MongoDB URI
const uri = process.env.MONGODB_URL;
const dbName = 'upwork';
const collectionName = 'jobs';

export async function storeJobsInMongoDB(jobs) {
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

                    createdDateTime: job?.jobTile?.job?.createTime ? new Date(job.jobTile.job.createTime) : null,
                    publishedDateTime: job?.jobTile?.job?.publishTime ? new Date(job.jobTile.job.publishTime) : null,

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

export async function getPendingJobs() {
    const client = new MongoClient(uri);
    try {
        await client.connect();
        const db = client.db(dbName);
        const collection = db.collection(collectionName);

        return collection
            .find({ status: "PENDING" })
            .project({ id: 1 })
            .toArray();

    } catch (err) {
        console.error('MongoDB error:', err);
    }
}

export async function enrichJob(job, jobDetails) {
    const client = new MongoClient(uri);

    await client.connect();
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

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
}

export async function setJobStatus(jobId, status) {
    const client = new MongoClient(uri);

    await client.connect();
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    await collection.updateOne(
        {id: jobId},
        {
            $set: {
                updatedAt: new Date(),
                status: status
            },
        });

}

function getAvgHireRate(postedCount, totalHires) {
    if(!postedCount || !totalHires) return 0;
    if(totalHires >= postedCount) return 100;

    return totalHires * 100 / postedCount;

}
