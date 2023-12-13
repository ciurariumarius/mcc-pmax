// Define Sheets
const e = SpreadsheetApp.openByUrl("");
const t = e.getSheetByName("r_c"), s = e.getSheetByName("r_ca"), a = e.getSheetByName("r_a"), i = e.getSheetByName("r_ag");

// Main Function
function main() {
    // Get all accounts id 
    const ids = getAccountIds();
    // Clear sheets
    t.getRange("A2:K").clearContent(), s.getRange("A2:I").clearContent(), a.getRange("A2:K").clearContent(), i.getRange("A2:H").clearContent();
    // For every batch of 50 ids
    for (let j = 0; j < ids.length; j++) {
        let accountIds = ids[j];
        // select accounts
        let accountSelector = AdsManagerApp
        .accounts()
        .withIds(accountIds);
        // process accounts, than push data
        accountSelector.executeInParallel("processAccounts", "pushData");
  }
}

function processAccounts(){

    let c = AdsApp.currentAccount().getCustomerId(),r=c;
    try{
    r = AdsApp.currentAccount().getName()
    }
    catch(g){}
    Logger.log(c);
    let o=[],m=AdsApp.search(`
        SELECT 
        campaign.name, 
        campaign.id, 
        metrics.impressions,
        metrics.clicks,
        metrics.cost_micros,
        metrics.conversions,
        metrics.conversions_value,
        metrics.video_views, 
        metrics.average_cpv

        FROM campaign
        WHERE segments.date DURING LAST_30_DAYS
        AND campaign.advertising_channel_type = "PERFORMANCE_MAX" 
        AND metrics.cost_micros > 0
        ORDER BY campaign.id
    `),u=[];
    for(;m.hasNext();){
        let l=m.next(),
        {resourceName:p,name:h,id:R}=l.campaign,
        {costMicros:d,impressions:A,clicks:N,conversions:E,conversionsValue:v,videoViews:y,averageCpv:D}=l.metrics;
        o.push(R),
        u.push([r,c,h,R,d/1e6,A,N,E,v,y,D/1e6]);
    }
    if (u.length === 0) {
        return;
    }
      
    let S=AdsApp.search(`
        SELECT 
        campaign.name,
        campaign.id, 
        metrics.impressions,
        metrics.clicks,
        metrics.cost_micros,
        metrics.conversions,
        metrics.conversions_value,
        segments.asset_interaction_target.asset, 
        segments.asset_interaction_target.interaction_on_this_asset
        
        FROM campaign
        WHERE segments.date DURING LAST_30_DAYS
        AND campaign.advertising_channel_type = "PERFORMANCE_MAX" 
        AND segments.asset_interaction_target.interaction_on_this_asset != "TRUE"
        AND metrics.cost_micros > 0
        ORDER BY campaign.id
    `),$=[];
    for(;S.hasNext();){
        let f=S.next(),{name:x,id:I}=f.campaign,{assetInteractionTarget:{asset:_}}=f.segments,{costMicros:C,impressions:L,clicks:O,conversions:T,conversionsValue:V}=f.metrics;
        $.push([r,x,I,_,C/1e6,L,O,T,V])
    }

    let b=AdsApp.search(`
        SELECT 
        campaign.name, 
        campaign.id, 
        asset_group.id, 
        asset_group.name, 
        asset_group.status, 
        asset_group_listing_group_filter.type, 
        metrics.impressions,
        metrics.clicks,
        metrics.cost_micros,
        metrics.conversions,
        metrics.conversions_value
        
        FROM asset_group_product_group_view 
        WHERE segments.date DURING LAST_30_DAYS 
        AND asset_group_listing_group_filter.type != "SUBDIVISION" 
        AND campaign.id IN ('${o.join("','")}')
        AND metrics.cost_micros > 0
        ORDER BY campaign.id
    `),B=[];
    for(;b.hasNext();){
        let M=b.next(),{name:w,id:F,status:U}=M.assetGroup,{costMicros:Y,impressions:G,clicks:H,conversions:W,conversionsValue:k}=M.metrics;
        B.push([r,M.campaign.name,M.campaign.id,w,F,U,Y/1e6,G,H,W,k])
    }

    let j=AdsApp.search(`
        SELECT 
        campaign.name, 
        campaign.id, 
        asset_group.id, 
        asset_group.name, 
        asset.resource_name,
        asset_group_asset.field_type,
        asset.source,
        asset.name,
        asset.text_asset.text,
        asset.image_asset.full_size.url,
        asset.youtube_video_asset.youtube_video_title,
        asset.youtube_video_asset.youtube_video_id
    
        FROM asset_group_asset 
        WHERE campaign.id IN ('${o.join("','")}')
    `),z=[];
    for(;j.hasNext();){
        let K=j.next(),P="",X="",q="";K.asset.imageAsset&&(P=K.asset.imageAsset.fullSize.url),K.asset.youtubeVideoAsset&&(X=K.asset.youtubeVideoAsset.youtubeVideoId,q=K.asset.youtubeVideoAsset.youtubeVideoTitle);let{resourceName:J,source:Q,name:Z}=K.asset,{fieldType:ee}=K.assetGroupAsset;
        z.push([r,J,ee,Q,P,X,q,Z])
    }

    return [u, $, B, z];
}

function pushData(results) {
    const sheets = {
        0: t,
        1: s,
        2: a,
        3: i,
    };
    
    for (let j = 0, length = results.length; j < length; j++) {
        let account = results[j].getReturnValue();
        if (account) {   
            // Parse the JSON string to get the array
            try {
                let accountDataArray = JSON.parse(account);
                // Iterate through each result array
                for (let k = 0; k < accountDataArray.length; k++) {
                    let accountData = accountDataArray[k];
                    if (accountData.length > 0) {
                        let sheet = sheets[k];
                        // Check if the sheet is defined
                        if (sheet) {
                            sheet.getRange(sheet.getLastRow() + 1, 1, accountData.length, accountData[0].length).setValues(accountData);
                        }
                    }
                } // end for
            } catch (error) {
                Logger.log('Error parsing JSON:', error);
                continue; 
            }
        } // endif
    }
}

function getAccountIds() {
  const chunkSize = 50;
  let accountIterator = AdsManagerApp.accounts().get();
  let accountIds = [];
  
  while (accountIterator.hasNext()) {
    let account = accountIterator.next();
    AdsManagerApp.select(account);
    accountIds.push(account.getCustomerId());
  }

  // Split the array into chunks
  let chunkedAccounts = [];
  for (let i = 0; i < accountIds.length; i += chunkSize) {
    let chunk = accountIds.slice(i, i + chunkSize);
    chunkedAccounts.push(chunk);
  }

  return chunkedAccounts;
}