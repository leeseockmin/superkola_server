 #region Ranking
 public static async Task OnCS_GetRankingDatas(UserSession session, FlatMessage message)
 {
     var req = message.MessageAs<CS_GetRankingDatas>();

     DateTime now = ServerTime.Now;
     var builder = new FlatBufferBuilder(1);
     EResultType resultType = EResultType.Success;
     Offset<SC_GetRankingDatas> messagePacketData = new Offset<SC_GetRankingDatas>();
     do
     {
         try
         {
             var getAllRankingStatus = await RedisRanking.GetGlobalRankingState();
             if (getAllRankingStatus == RankingState.Maintainance)
             {
                 messagePacketData = new Offset<SC_GetRankingDatas>();
                 resultType = EResultType.NotProgressRanking;
                 break;
             }

             var getRankingMetaDatas = MetaData<TableRankingMeta>.Data.GetRankingDatas(now);
             if (getRankingMetaDatas == null || getRankingMetaDatas.Count() <= 0)
             {
                 messagePacketData = new Offset<SC_GetRankingDatas>();
                 resultType = EResultType.NotProgressRanking;
                 break;
             }
             using var connectionWrapper = await DatabaseManager.Instance.GetDisposableConnectionAsync(EDatabaseType.Game);
             var connection = connectionWrapper.Value;
             OffsetArrayBuilder<FlatMessages.Datas.RankingData> arrayBuilder = new OffsetArrayBuilder<FlatMessages.Datas.RankingData>(getRankingMetaDatas.Count);
             foreach (var getRankingData in getRankingMetaDatas)
             {

                 var getRankingStatusInfo = await RankingLogic.GetRankingStatusInfo(getRankingData, now);
                 var getUserRanking = await RankingLogic.GetUserRankingInfo(getRankingData.Index, session.Actor.UserModel.UserIndex);
                 if (getUserRanking.isNew == true)
                 {
                     RedisRanking.RankingUserScore redisRankingUserScore = null;
                     var getUserDbRanking = await DbGameUserRanking.GetUserRankingMissionInfo(connection, getRankingData.Index, session.Actor.UserModel.UserIndex);
                     if (getUserDbRanking != null)
                     {
                         redisRankingUserScore = new RedisRanking.RankingUserScore()
                         {
                             score = getUserDbRanking.score,
                             seasonNo = getRankingData.Index,
                             updateTime = getUserDbRanking.updateTime,
                             userIdx = getUserDbRanking.userIdx,
                         };
                     }
                     else
                     {
                         redisRankingUserScore = new RedisRanking.RankingUserScore()
                         {
                             score = 0,
                             seasonNo = getRankingData.Index,
                             updateTime = now,
                             userIdx = session.Actor.UserModel.UserIndex,
                         };
                     }
                     await RedisRanking.SetRankingUserScore(redisRankingUserScore);
                 }
                 bool isReward = false;
                 if (getRankingStatusInfo.rankingStatus == RankingState.Reward)
                 {
                     isReward = await RedisRanking.IsSeasonRankingReward(getRankingData.Index, session.Actor.UserModel.UserIndex);
                 }

                 arrayBuilder.Add(FlatMessages.Datas.RankingData.CreateRankingData(builder, getRankingStatusInfo.seasonNo, (int)getRankingStatusInfo.rankingStatus, isReward));

             }
             var vectorOffset = SC_GetRankingDatas.CreateGetRankingDatasVector(builder, arrayBuilder.GetArray());
             messagePacketData = SC_GetRankingDatas.CreateSC_GetRankingDatas(builder, vectorOffset);
         }
         catch (Exception ex)
         {
             Logger.ErrorLog(ex);
             resultType = EResultType.LogicError;
         }
     }
     while (false);

     session.Actor.Send(builder, EMessageType.SC_GetRankingDatas, messagePacketData, resultType);
 }

 public static async Task OnCS_GetSummaryRankingMyInfo(UserSession session, FlatMessage message)
 {
     var req = message.MessageAs<CS_GetSummaryRankingMyInfo>();
     DateTime now = ServerTime.Now;
     Offset<SC_GetSummaryRankingMyInfo> messagePacketData = new Offset<SC_GetSummaryRankingMyInfo>();
     var builder = new FlatBufferBuilder(1);
     EResultType resultType = EResultType.Success;
     do
     {
         try
         {
             bool isReward = false;
             var getAllRankingStatus = await RedisRanking.GetGlobalRankingState();
             if (getAllRankingStatus == RankingState.Maintainance)
             {
                 Logger.ErrorLog($"OnCS_GetSummaryRankingMyInfo Global Ranking Maintance UserIdx :{session.Actor.UserModel.UserIndex} ");
                 resultType = EResultType.NotProgressRanking;
                 break;
             }

             var getRankingMetaDatas = MetaData<TableRankingMeta>.Data.GetRankingDatas(now);
             if (getRankingMetaDatas == null || getRankingMetaDatas.Count() <= 0)
             {
                 Logger.ErrorLog($"OnCS_GetSummaryRankingMyInfo Not Progress Ranking UserIdx :{session.Actor.UserModel.UserIndex} ");
                 resultType = EResultType.NotProgressRanking;
                 break;
             }

             OffsetArrayBuilder<FlatMessages.Datas.SummaryMyInfo> arrayBuilder = new OffsetArrayBuilder<FlatMessages.Datas.SummaryMyInfo>(getRankingMetaDatas.Count);
             foreach (var getRankingData in getRankingMetaDatas)
             {

                 var getUserRankingInfo = await RankingLogic.GetUserRankingInfo(getRankingData.Index, session.Actor.UserModel.UserIndex);
                 if (getUserRankingInfo.isNew == true)
                 {
                     resultType = EResultType.NotProgressRanking;
                     Logger.ErrorLog($"OnCS_GetSummaryRankingMyInfo Not Exsist RankingScore Info , SeasonId :{getRankingData.Index} , UserIdx :{session.Actor.UserModel.UserIndex} ");
                     break;
                 }

                 var getRankingStatusInfo = await RankingLogic.GetRankingStatusInfo(getRankingData, now);
                 if (getRankingStatusInfo.rankingStatus == RankingState.Reward)
                 {
                     isReward = await RedisRanking.IsSeasonRankingReward(getRankingData.Index, session.Actor.UserModel.UserIndex);
                 }
                 else if (getRankingStatusInfo.rankingStatus == RankingState.Calculate)
                 {
                     getUserRankingInfo.rank = 0;
                 }

                 arrayBuilder.Add(FlatMessages.Datas.SummaryMyInfo.CreateSummaryMyInfo(builder, getRankingData.Index, getUserRankingInfo.rank, getUserRankingInfo.score, isReward, (int)getRankingStatusInfo.rankingStatus));
             }

             var vectorOffset = SC_GetSummaryRankingMyInfo.CreateSummaryMyDatasVector(builder, arrayBuilder.GetArray());
             messagePacketData = SC_GetSummaryRankingMyInfo.CreateSC_GetSummaryRankingMyInfo(builder, vectorOffset);
         }
         catch (Exception ex)
         {
             Logger.ErrorLog(ex);
             resultType = EResultType.LogicError;
         }

     }
     while (false);

     session.Actor.Send(builder, EMessageType.SC_GetSummaryRankingMyInfo, messagePacketData, resultType);
 }


 public static async Task OnCS_GetRankingUserList(UserSession session, FlatMessage message)
 {
     var req = message.MessageAs<CS_GetRankingUserList>();
     DateTime now = ServerTime.Now;
     var builder = new FlatBufferBuilder(1);
     EResultType resultType = EResultType.Success;
     RankingState rankState = RankingState.Progresss;
     bool isReward = false;
     Offset<SC_GetRankingUserList> messagePacketData = new Offset<SC_GetRankingUserList>();
     do
     {
         try
         {

             var getAllRankingStatus = await RedisRanking.GetGlobalRankingState();
             if (getAllRankingStatus == RankingState.Maintainance)
             {
                 resultType = EResultType.NotProgressRanking;
                 Logger.ErrorLog($"OnCS_GetRankingUserList Ranking Maintainance UserIdx :{session.Actor.UserModel.UserIndex} ");
                 rankState = getAllRankingStatus;
                 session.Actor.Send(builder, EMessageType.SC_GetRankingUserList, messagePacketData, resultType);
                 break;
             }

             var getRankingMetaData = MetaData<TableRankingMeta>.Data.GetRankingData(req.SeasonNo, now);
             if (getRankingMetaData == null)
             {
                 resultType = EResultType.NotProgressRanking;
                 Logger.ErrorLog($"OnCS_GetRankingUserList Ranking Not Progress SeasonNo :{req.SeasonNo},  UserIdx :{session.Actor.UserModel.UserIndex} ");
                 rankState = RankingState.NotProgress;
                 session.Actor.Send(builder, EMessageType.SC_GetRankingUserList, messagePacketData, resultType);
                 break;
             }
             else
             {
                 var getRankingStatusInfo = await RankingLogic.GetRankingStatusInfo(getRankingMetaData, now);
                 rankState = getRankingStatusInfo.rankingStatus;

                 var getUserRankingInfo = await RankingLogic.GetUserRankingInfo(req.SeasonNo, session.Actor.UserModel.UserIndex);
                 if (getUserRankingInfo.isNew == true)
                 {
                     resultType = EResultType.NotProgressRanking;
                     Logger.ErrorLog($"OnCS_GetRankingUserList Not Exsist RankingScore Info , SeasonId :{req.SeasonNo} , UserIdx :{session.Actor.UserModel.UserIndex} ");
                     break;
                 }

                 if (rankState != RankingState.Maintainance && rankState != RankingState.NotProgress)
                 {
                     if (rankState == RankingState.Reward)
                     {
                         isReward = await RedisRanking.IsSeasonRankingReward(req.SeasonNo, session.Actor.UserModel.UserIndex);
                     }
                     else if (rankState == RankingState.Calculate)
                     {
                         getUserRankingInfo.rank = 0;
                     }

                     OffsetArrayBuilder<FlatMessages.Datas.RankingUserData> arrayBuilder = new OffsetArrayBuilder<RankingUserData>();
                     var getRankUserInfos = GlobalModel.GetRankingUserInfos(req.SeasonNo);
                     if (getRankUserInfos != null && getRankUserInfos.Count() > 0)
                     {
                         var userRankingCount = getRankUserInfos.Count();
                         int maxCount = 20;
                         int page = userRankingCount / maxCount;
                         int extraCount = userRankingCount % maxCount;
                         if (extraCount > 0)
                         {
                             ++page;
                         }

                         int startIndex = 0;
                         bool isLast = false;

                         var getLastRanking = MetaData<TableRankingRewardMeta>.Data.GetseasonLastRanking(getRankingMetaData.RankingRewardGroupId);

                         while (page > 0)
                         {
                             if (page == 1 && maxCount >= extraCount)
                             {
                                 if (extraCount != 0)
                                 {
                                     maxCount = extraCount;
                                 }
                                 isLast = true;
                             }

                             arrayBuilder = new OffsetArrayBuilder<RankingUserData>(maxCount);
                             int lastCount = startIndex + maxCount;

                             for (int i = startIndex; i < lastCount; i++)
                             {
                                 var targetUser = getRankUserInfos[i];
                                 var convertNickName = builder.CreateString(targetUser.nickName);
                                 var rankData = RankingUserData.CreateRankingUserData(builder, targetUser.userIdx, targetUser.rank, convertNickName, targetUser.profileImage, targetUser.score, targetUser.accIdx);
                                 arrayBuilder.Add(rankData);
                             }

                             var vectorOffset = SC_GetRankingUserList.CreateRankingUserDatasVector(builder, arrayBuilder.GetArray());
                             messagePacketData = SC_GetRankingUserList.CreateSC_GetRankingUserList(builder, isLast, getRankingStatusInfo.seasonNo, (int)getRankingStatusInfo.rankingStatus, isReward, getUserRankingInfo.rank, getUserRankingInfo.score, vectorOffset);
                             session.Actor.Send(builder, EMessageType.SC_GetRankingUserList, messagePacketData, resultType);

                             startIndex = lastCount;
                             --page;
                         }
                     }
                     else
                     {
                         messagePacketData = SC_GetRankingUserList.CreateSC_GetRankingUserList(builder, true, getRankingStatusInfo.seasonNo, (int)getRankingStatusInfo.rankingStatus, isReward, getUserRankingInfo.rank, getUserRankingInfo.score, new VectorOffset());
                         session.Actor.Send(builder, EMessageType.SC_GetRankingUserList, messagePacketData, resultType);
                     }
                 }
                 else
                 {
                     messagePacketData = SC_GetRankingUserList.CreateSC_GetRankingUserList(builder, true, getRankingStatusInfo.seasonNo, (int)getRankingStatusInfo.rankingStatus, isReward, getUserRankingInfo.rank, getUserRankingInfo.score, new VectorOffset());
                     session.Actor.Send(builder, EMessageType.SC_GetRankingUserList, messagePacketData, resultType);
                 }
             }
         }
         catch (Exception ex)
         {
             resultType = EResultType.LogicError;
             Logger.ErrorLog($"{ex}");
             session.Actor.Send(builder, EMessageType.SC_GetRankingUserList, messagePacketData, resultType);
         }
     }
     while (false);

 }

 public static async Task OnCS_GetRankingReward(UserSession session, FlatMessage message)
 {
     var req = message.MessageAs<CS_GetRankingReward>();
     DateTime now = ServerTime.Now;
     var builder = new FlatBufferBuilder(1);
     EResultType resultType = EResultType.Success;
     Offset<SC_GetRankingReward> messagePacketData = new Offset<SC_GetRankingReward>();
     bool isReward = true;

     using var connectionWrapper = await DatabaseManager.Instance.GetDisposableConnectionAsync(EDatabaseType.Game);
     var connection = connectionWrapper.Value;
     connection.BeginTransaction();
     do
     {
         try
         {
             var getAllRankingStatus = await RedisRanking.GetGlobalRankingState();
             if (getAllRankingStatus == RankingState.Maintainance)
             {
                 Logger.ErrorLog($"OnCS_GetRankingReward Global Ranking Maintainance SeasonNo : {req.SeasonNo} , userIdx : {session.Actor.UserModel.UserIndex}", false);
                 resultType = EResultType.NotProgressRanking;
                 break;
             }

             var getRankingMetaData = MetaData<TableRankingMeta>.Data.GetRankingData(req.SeasonNo, now);
             if (getRankingMetaData == null)
             {
                 resultType = EResultType.NotProgressRanking;
                 Logger.ErrorLog($"OnCS_GetRankingReward Not ProgressRaking SeasonNo : {req.SeasonNo} , userIdx : {session.Actor.UserModel.UserIndex}", false);
                 break;
             }

             var getRankingStatusInfo = await RankingLogic.GetRankingStatusInfo(getRankingMetaData, now);
             if (getRankingStatusInfo == null)
             {
                 resultType = EResultType.MetaDataError;
                 Logger.ErrorLog($"OnCS_GetRankingReward Ranking Data is Null : SeasonNo : {getRankingMetaData.Index} ,  UserIdx :{session.Actor.UserModel.UserIndex} , Status : None", false);
                 break;
             }

             if (getRankingStatusInfo.rankingStatus != RankingState.Reward)
             {
                 resultType = EResultType.NotRankingRewardTime;
                 Logger.ErrorLog($"Ranking Service Not Reward Time : SeasonNo : {getRankingMetaData.Index} , UserIdx :{session.Actor.UserModel.UserIndex} , RankingState: {getRankingStatusInfo.rankingStatus}", false);
                 break;
             }

             // 랭킹 보상 있나 여부 확인 후 없으면 에러

             var userRank = await RedisRanking.GetSeasonRanking(req.SeasonNo, session.Actor.UserModel.UserIndex);
             if (userRank <= 0)
             {
                 resultType = EResultType.AlreadyRankingReward;
                 Logger.ErrorLog($"OnCS_GetRankingReward GetSeasonRanking Already Raking Reward: SeasonNo : {getRankingMetaData.Index} , UserIdx :{session.Actor.UserModel.UserIndex}", false);
                 break;
             }

             var isReceiveReward = await RedisRanking.DeleteSeasonRankingReward(req.SeasonNo, session.Actor.UserModel.UserIndex);
             if (isReceiveReward == false)
             {
                 resultType = EResultType.AlreadyRankingReward;
                 Logger.ErrorLog($"OnCS_GetRankingReward DeleteSeasonRankingReward Already Raking Reward: SeasonNo : {getRankingMetaData.Index} , UserIdx :{session.Actor.UserModel.UserIndex}", false);
                 break;
             }

             var getRankingReward = MetaData<TableRankingRewardMeta>.Data.GetRewardMetaDatas(userRank, getRankingMetaData.RankingRewardGroupId);
             if (getRankingReward != null)
             {
                 List<Contents.Model.ItemModel.ItemInfo> itemInfos = new List<Contents.Model.ItemModel.ItemInfo>();
                 for (int i = 0; i < getRankingReward.RankingRewardCount.Count; ++i)
                 {
                     itemInfos.Add(new Contents.Model.ItemModel.ItemInfo(getRankingReward.RankingRewardId[i], getRankingReward.RankingRewardCount[i]));
                 }

                 StringBuilder stringBuilder = new StringBuilder();
                 stringBuilder.Append(req.SeasonNo);
                 stringBuilder.Append(",");
                 stringBuilder.Append(userRank);
                 var result = await Shop.NewMail(connection, session.Actor, itemInfos, getRankingMetaData.RankingMailTitle, getRankingMetaData.RankingMailDesc, stringBuilder.ToString(), MailType.Rank, now.AddDays(7));
                 if (result.Item1 != EResultType.Success && result.Item2 <= 0)
                 {
                     resultType = EResultType.DatabaseError;
                     Logger.ErrorLog($"OnCS_GetRankingReward Raking Add New Mail Fail : SeasonNo : {getRankingMetaData.Index} , UserIdx :{session.Actor.UserModel.UserIndex}", false);
                     isReward = false;
                 }
                 else
                 {
                     messagePacketData = SC_GetRankingReward.CreateSC_GetRankingReward(builder, userRank);
                 }
             }

             connection.Commit();
         }
         catch (Exception ex)
         {
             resultType = EResultType.LogicError;
             Logger.ErrorLog($"{ex.Message}", false);
         }
     }
     while (false);

     if (isReward == false)
     {
         connection.Rollback();
     }
     session.Actor.Send(builder, EMessageType.SC_GetRankingReward, messagePacketData, resultType);
 }
 #endregion
