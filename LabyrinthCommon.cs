using BaseMetaTables;
using Common.Database.Connections;
using Common.SQS;
using Common.SQS.LogModel;
using GameCommon.Entity.Game;
using GameCommon.Entity.Game.Logic;
using GameCommon.Resources.RefinedResources;
using GameCommon.Utils;
using GameServer.Contents.Model;
using GameServer.Contents.User;
using GameServer.Session;
using MasterData.Types;
using Newtonsoft.Json;

namespace GameServer.Contents.Common
{
    public class LabyrinthCommon
    {
        public class StageNodeData
        {
            public int nodeStage { get; set; }
            public int nodeCount { get; set; }
            public List<LabyrinthNode> nodeDatas { get; set; }
        }
        public static async Task<List<LabyrinthNode>> CreateLabyrinthNode(BaseDatabaseConnection connection, long userIdx, int floor, DateTime now)
        {
            List<LabyrinthNode> nodeDatas = new List<LabyrinthNode>();
            var getMaxNodes = MetaData<TableGameConfigMeta>.Data.GetRefineGameConfig(MasterData.Types.ConfigType.LABYRINTH_MAX_NODE);
            if (getMaxNodes.Count <= 1)
            {
                Logger.ErrorLog($"CreateLabyrinthNode Max Node Error Count :{getMaxNodes.Count}");
                return null;
            }

            var getfirstBranchCount = MetaData<TableGameConfigMeta>.Data.GetIntConf(MasterData.Types.ConfigType.LABYRINTH_START_BRANCH);
            if (getfirstBranchCount <= 0)
            {
                Logger.ErrorLog($"CreateLabyrinthNode firstBranchCount Error Count :{getfirstBranchCount}");
                return null;
            }
            var getDepthCount = MetaData<TableGameConfigMeta>.Data.GetIntConf(MasterData.Types.ConfigType.LABYRINTH_MAX_DEPTH);
            if (getDepthCount <= 0)
            {
                Logger.ErrorLog($"CreateLabyrinthNode MaxDepth Error Count :{getDepthCount}");
                return null;
            }

            var getMaxNodeCount = MetaData<TableGameConfigMeta>.Data.GetIntConf(MasterData.Types.ConfigType.LABYRINTH_MAX_BRANCH);
            if (getMaxNodeCount <= 0)
            {
                Logger.ErrorLog($"CreateLabyrinthNode getMaxNodeCount Error Count :{getMaxNodeCount}");
                return null;
            }

            var getLastNodeCount = MetaData<TableGameConfigMeta>.Data.GetIntConf(MasterData.Types.ConfigType.LABYRINTH_END_NODE_COUNT);
            if (getLastNodeCount <= 0)
            {
                Logger.ErrorLog($"CreateLabyrinthNode getLastNodeCount Error Count :{getLastNodeCount}");
                return null;
            }

            var getMaxCount = RandomGenerator.GetRandomValue(getMaxNodes[0], getMaxNodes[1] + 1);
            int maxNodeCreateCount = 2;

            getMaxCount = getMaxCount - (getfirstBranchCount + getLastNodeCount);

            int stage = 1;
            List<StageNodeData> stageNodeDatas = new List<StageNodeData>();
            stageNodeDatas.Add(new StageNodeData()
            {
                nodeStage = stage,
                nodeCount = getfirstBranchCount,
                nodeDatas = new List<LabyrinthNode>()
            });

            var getNodes = GetNodeCount(getDepthCount - 2, getMaxCount, getMaxNodeCount, maxNodeCreateCount);
            foreach (var nodeCount in getNodes)
            {
                stage = stage + 1;
                stageNodeDatas.Add(new StageNodeData()
                {
                    nodeStage = stage,
                    nodeCount = nodeCount,
                    nodeDatas = new List<LabyrinthNode>()
                });
            }

            stageNodeDatas.Add(new StageNodeData()
            {
                nodeStage = getDepthCount,
                nodeCount = getLastNodeCount,
                nodeDatas = new List<LabyrinthNode>()
            });

            // 먼저 연결한다.
            for (int i = 0; i < stageNodeDatas.Count; ++i)
            {
                var stageNodeData = stageNodeDatas[i];
                int nextNodeCount = 0;
                if (i != stageNodeDatas.Count - 1)
                {
                    var nextStageNodeCount = stageNodeDatas[i + 1];
                    nextNodeCount = nextStageNodeCount.nodeCount;
                }
                else
                {
                    nextNodeCount = getLastNodeCount;
                }

                stageNodeData.nodeDatas = new List<LabyrinthNode>();
                int lastRow = 0;
                int tempRandLinkdCount = nextNodeCount;
                bool isLastRow = false;
                for (int j = 0; j < stageNodeData.nodeCount; ++j)
                {
                    var nodeData = new LabyrinthNode();
                    nodeData.floor = floor;
                    nodeData.userIdx = userIdx;
                    nodeData.nodeRow = j;
                    nodeData.nodeStage = stageNodeData.nodeStage;
                    nodeData.createTime = now;
                    nodeData.monsterData = "{}";
                    if (isLastRow == false)
                    {
                        int randLinkndCount = RandomGenerator.GetRandomValue(1, tempRandLinkdCount + 1);
                        if (j == 0)
                        {
                            nodeData.startLinkRow = lastRow;
                            nodeData.endLinkRow = randLinkndCount - 1;
                            if (j + 1 == stageNodeData.nodeCount)
                            {
                                nodeData.endLinkRow = nextNodeCount - 1;
                            }
                        }
                        else if (j + 1 == stageNodeData.nodeCount)
                        {
                            var randBeforeRow = RandomGenerator.GetRandomValue(0, 2);
                            if (randBeforeRow == 0)
                            {
                                nodeData.startLinkRow = lastRow + 1;
                            }
                            else
                            {
                                nodeData.startLinkRow = lastRow;

                            }
                            nodeData.endLinkRow = nextNodeCount - 1;
                        }
                        else
                        {
                            var randBeforeRow = RandomGenerator.GetRandomValue(0, 2);
                            if (randBeforeRow == 0)
                            {
                                nodeData.startLinkRow = lastRow + 1;
                            }
                            else
                            {
                                nodeData.startLinkRow = lastRow;
                            }

                            nodeData.endLinkRow = nodeData.startLinkRow + (randLinkndCount - 1);
                            if (nodeData.endLinkRow > nextNodeCount - 1)
                            {
                                nodeData.endLinkRow = nextNodeCount - 1;
                            }
                        }

                        lastRow = nodeData.endLinkRow;
                        if (lastRow == nextNodeCount - 1)
                        {
                            isLastRow = true;
                        }

                        tempRandLinkdCount -= (nodeData.endLinkRow - nodeData.startLinkRow);
                        if (tempRandLinkdCount <= 0)
                        {
                            tempRandLinkdCount = 0;
                        }
                    }
                    else
                    {
                        nodeData.startLinkRow = lastRow;
                        nodeData.endLinkRow = lastRow;
                    }

                    stageNodeData.nodeDatas.Add(nodeData);
                }
            }

            foreach (var stageNodeData in stageNodeDatas)
            {
                foreach (var nodeData in stageNodeData.nodeDatas)
                {
                    var isSuccess = GetNodeMetaInfo(floor, stageNodeData.nodeStage, nodeData);
                    if (isSuccess == false)
                    {
                        Logger.ErrorLog($"CreateLabyrinthNode GetNodeMetaInfo Error Floor :{floor} , Stage :{stageNodeData.nodeStage}");
                        return null;
                    }
                }
                nodeDatas.AddRange(stageNodeData.nodeDatas);
            }

            return nodeDatas;
        }

        private static bool GetNodeMetaInfo(int floor, int stage, LabyrinthNode nodedata)
        {
            var getStageInfos = MetaData<TableLabyrinthMeta>.Data.GetLabyrinthMetas(floor, stage);
            if (getStageInfos == null || getStageInfos.Count <= 0)
            {
                Logger.ErrorLog($"GetNodeMetaInfo StageInfo Is Null  Floor : {floor}");
                return false;
            }

            var getProb = MetaData<TableLabyrinthMeta>.Data.GetProb(getStageInfos);
            if (getProb <= 0)
            {
                Logger.ErrorLog($"GetNodeMetaInfo Prob Error Floor : {floor}, Stage :{stage}");
                return false;
            }

            try
            {
                var prob = RandomGenerator.GetRandomValue(0, getProb + 1);
                var getLabyrinthMeta = MetaData<TableLabyrinthMeta>.Data.RandomLabyrinthMeta(getStageInfos, prob);
                if (getLabyrinthMeta.NodeType != MasterData.Types.NodeType.Artifact)
                {
                    var getlabyrinthFormationMetas = MetaData<TableLabyrinthFormationMeta>.Data.GetStageInfos(getLabyrinthMeta.CreatureSectorGroupId);
                    if (getlabyrinthFormationMetas == null || getlabyrinthFormationMetas.Count <= 0)
                    {
                        Logger.ErrorLog($"GetNodeMetaInfo getlabyrinthFormationMetas Error Floor : {floor}, Stage :{stage}, CreatureSectorGroupId : {getLabyrinthMeta.CreatureSectorGroupId}");
                        return false;
                    }
                    int totalprob = getlabyrinthFormationMetas.Select(i => i.SectorProb).Sum();
                    prob = RandomGenerator.GetRandomValue(0, totalprob + 1);
                    int stageIndex = 0;
                    for (int i = 0; i < getlabyrinthFormationMetas.Count; ++i)
                    {
                        prob -= getlabyrinthFormationMetas[i].SectorProb;
                        if (prob <= 0)
                        {
                            stageIndex = i;
                            break;
                        }
                    }

                    var getRandStageInfo = getlabyrinthFormationMetas[stageIndex];
                    nodedata.labyrinthFormationMetaId = getRandStageInfo.Index;
                }
                else
                {
                    nodedata.labyrinthFormationMetaId = 0;
                }
                nodedata.labyrinthMetaId = getLabyrinthMeta.Index;
                nodedata.state = NodeState.Idle;
                return true;
            }
            catch (Exception ex)
            {
                Logger.ErrorLog(ex);
            }

            return false;
        }

        private static int[] GetNodeCount(int stageCount, int allNodeCount, int maxCount, int maxLimitCount = 0)
        {
            int[] nodes = new int[stageCount];
            int remaining = allNodeCount;
            int currentMaxCount = 0;

            for (int i = 0; i < stageCount; i++)
            {
                // 각 요소가 가질 수 있는 최대값 계산
                int maxValue = Math.Min(maxCount, remaining - (stageCount - 1 - i));

                // 최대값의 개수가 maxFive 이상일 경우, 최대값을 maxCount - 1로 제한
                if (currentMaxCount >= maxLimitCount)
                {
                    maxValue = Math.Min(maxValue, maxCount - 1);
                }

                Random rand = new Random();
                // 1부터 maxValue 사이의 랜덤 값 할당
                nodes[i] = rand.Next(1, maxValue + 1);

                // 최대값이 나오면 현재 최대값의 개수 증가
                if (nodes[i] == maxCount)
                {
                    currentMaxCount++;
                }

                remaining -= nodes[i];
            }

            // 5의 개수를 제한하며 남은 값을 재분배
            int limitCount = nodes.Count(x => x == maxCount);
            int index = 0;

            while (index < stageCount && remaining > 0)
            {
                int maxPossibleAdd = maxCount - nodes[index];

                // 5의 개수가 2개 이상이면, 5를 추가할 수 없도록 제한
                if (nodes[index] == maxCount && limitCount >= maxLimitCount)
                {
                    maxPossibleAdd = Math.Min(maxPossibleAdd, (maxCount - 1) - nodes[index]);
                }

                int add = Math.Min(remaining, maxPossibleAdd);
                nodes[index] += add;
                remaining -= add;

                // 5를 추가하면 5의 개수 증가
                if (nodes[index] == maxCount)
                {
                    limitCount++;
                }

                index++;
            }

            return nodes;
        }

        public class LabyrintRefreshInfo
        {
            public bool isResetSeason { get; set; }
            public long labyrinthSessionKey { get; set; }
            public bool isNewbie { get; set; }
        }

        public static async Task<LabyrintRefreshInfo> RefreshLabyrint(BaseDatabaseConnection connection, UserSession session, DateTime now, bool isRefresh = false, bool isGiveUp = false)
        {
            LabyrintRefreshInfo info = new LabyrintRefreshInfo();
            var getRefreshEnterCount = MetaData<TableGameConfigMeta>.Data.GetIntConf(MasterData.Types.ConfigType.Labyrinth_Daily_Reset_Count);
            if (getRefreshEnterCount <= 0)
            {
                Logger.ErrorLog($"RefreshLabyrint getRefreshEnterCount is Zero : {getRefreshEnterCount}");
                return null;
            }

            info.labyrinthSessionKey = session.Actor.LabyrinthModel.GetLabyrinthSessionKey();
            if (info.labyrinthSessionKey <= 0)
            {
                info.labyrinthSessionKey = Uniquekey.Get();
                session.Actor.LabyrinthModel.SetLabyrinthSessionKey(info.labyrinthSessionKey);
            }
            bool isDbUPdate = false;

            var nowDate = now.Date;

            var getUserLabyrinth = session.Actor.LabyrinthModel.GetUserLabyrinth();
            if (getUserLabyrinth == null)
            {
                info.isNewbie = true;
                getUserLabyrinth = new GameCommon.Entity.Game.Labyrinth()
                {
                    userIdx = session.Actor.UserModel.UserIndex,
                    floor = 0,
                    entryCount = getRefreshEnterCount,
                    currentRow = 0,
                    nodeStage = 0,
                    totalBroochCount = 0,
                    weeklyPoint = 0,
                    useCubeBatteryCount = 0,
                    weeklyRefreshTime = now,
                    dailyRefreshTime = now
                };
                isDbUPdate = true;
            }
            else
            {
                if (isGiveUp == false)
                {
                    if (getUserLabyrinth.dailyRefreshTime.Date < nowDate)
                    {
                        getUserLabyrinth.dailyRefreshTime = now;
                        getUserLabyrinth.entryCount = getRefreshEnterCount;
                        getUserLabyrinth.useCubeBatteryCount = 0;
                        isDbUPdate = true;
                    }

                    if (now.GetDayOfWeekDate(DayOfWeek.Monday).Date > getUserLabyrinth.weeklyRefreshTime.GetDayOfWeekDate(DayOfWeek.Monday).Date)
                    {
                        getUserLabyrinth.floor = 0;
                        getUserLabyrinth.currentRow = 0;
                        getUserLabyrinth.nodeStage = 0;
                        getUserLabyrinth.weeklyPoint = 0;
                        getUserLabyrinth.totalBroochCount = 0;
                        getUserLabyrinth.weeklyRefreshTime = now;
                        info.isResetSeason = true;
                        info.labyrinthSessionKey = Uniquekey.Get();
                        isDbUPdate = true;
                    }
                }
                else
                {
                    if (now.GetDayOfWeekDate(DayOfWeek.Monday).Date > getUserLabyrinth.weeklyRefreshTime.GetDayOfWeekDate(DayOfWeek.Monday).Date)
                    {
                        info.isResetSeason = true;
                    }
                    else
                    {
                        getUserLabyrinth.floor = 0;
                        getUserLabyrinth.currentRow = 0;
                        getUserLabyrinth.nodeStage = 0;
                        getUserLabyrinth.totalBroochCount = 0;
                        isDbUPdate = true;
                    }
                }
            }

            if (isDbUPdate == true && isRefresh == true)
            {
                var afftectd_cnt = await DBGameLabyrinthInfo.InsertOrUpdateLabyrinth(connection, getUserLabyrinth);
                if (afftectd_cnt > 0)
                {
                    bool reset = false;
                    if (info.isResetSeason == true || isGiveUp == true)
                    {
                        await DBGameLabyrinthCharacterInfo.ResetLabyrinthCharacter(connection, getUserLabyrinth.userIdx);
                        await DBGameLabyrinthArtifact.ResetArtifact(connection, getUserLabyrinth.userIdx, now);
                        await DBGameLabyrinthNodeInfo.ResetNode(connection, getUserLabyrinth.userIdx, now);
                        reset = true;
                    }
                    if (reset == true)
                    {
                        session.Actor.LabyrinthNodeModel.SetCurrentFloorNode(getUserLabyrinth.floor);
                        session.Actor.LabyrinthCharacterModel.LabyrintchCharacterClear();
                        session.Actor.LabyrinthArtifactModel.RsetLabyrithArtfact();
                        session.Actor.LabyrinthNodeModel.Clear();
                    }

                    session.Actor.LabyrinthModel.SetUserLabyrinth(getUserLabyrinth);
                    session.Actor.LabyrinthModel.SetLabyrinthSessionKey(info.labyrinthSessionKey);
                }
                else
                {
                    Logger.ErrorLog($"RefreshLabyrint DB Update Error UpdateLabyrinth UserIdx : {getUserLabyrinth.userIdx}");
                    return null;
                }
            }

            return info;
        }

        public class AtrticactRewardInfo
        {
            public EResultType resultType { get; set; }
            public List<int> artifactMetaIds { get; set; }
        }

        public static async Task<AtrticactRewardInfo> GetArtifactReward(int artifactGroupId)
        {
            AtrticactRewardInfo data = new AtrticactRewardInfo();
            data.resultType = EResultType.Success;

            var getRewardGroupMeta = MetaData<TableArtifactRewardGroupMeta>.Data.GetRewardGroupMeta(artifactGroupId);
            if (getRewardGroupMeta == null || getRewardGroupMeta.Count <= 0)
            {
                Logger.ErrorLog($"GetArtifactReward getRewardGorupMeta Is Null  GroupId :{artifactGroupId}");
                data.resultType = EResultType.LabyrinthGetArtifactRewardArtifactRewardGroupMetaFailed;
                return data;
            }
            List<MasterData.Common.ArtifactRewardGroupMeta> expectList = new List<MasterData.Common.ArtifactRewardGroupMeta>();
            data.artifactMetaIds = new List<int>();

            for (int i = 0; i < 3; ++i)
            {
                var getRewardGroupMetas = getRewardGroupMeta.Except(expectList).ToList();
                int totalProb = getRewardGroupMetas.Sum(i => i.ArtifactProb);
                var prob = RandomGenerator.GetRandomValue(1, totalProb + 1);
                foreach (var rewardData in getRewardGroupMetas)
                {
                    prob -= rewardData.ArtifactProb;
                    if (prob <= 0)
                    {
                        data.artifactMetaIds.Add(rewardData.ArtifactId);
                        expectList.Add(rewardData);
                        break;
                    }
                }
            }

            if (data.artifactMetaIds == null || data.artifactMetaIds.Count <= 0)
            {
                Logger.ErrorLog($"GetArtifactReward data Is Null");
                data.resultType = EResultType.LabyrinthGetArtifactRewarDataCheckFailed;
                return data;
            }
            return data;
        }

        public class PlayDataInfo
        {
            public List<LabyrinthNodeModel.NodeArtifactData> nodeArtifactDatas { get; set; }
            public List<LabyrinthNodeModel.NodeMonsterData> nodeMonsterDatas { get; set; }
        }

        public static async Task<PlayDataInfo> GetNodePlayData(NodeType type, int metaId)
        {
            PlayDataInfo playDataInfo = new PlayDataInfo();
            if (type == NodeType.Artifact)
            {
                playDataInfo.nodeArtifactDatas = new List<LabyrinthNodeModel.NodeArtifactData>();
                var artifactResult = await GetArtifactReward(metaId);
                if (artifactResult.resultType != EResultType.Success)
                {
                    return playDataInfo;
                }

                for (int i = 0; i < artifactResult.artifactMetaIds.Count; ++i)
                {
                    playDataInfo.nodeArtifactDatas.Add(new LabyrinthNodeModel.NodeArtifactData()
                    {
                        slotIdx = i,
                        artifactMetaId = artifactResult.artifactMetaIds[i]
                    });
                }
            }
            else
            {
                playDataInfo.nodeMonsterDatas = new List<LabyrinthNodeModel.NodeMonsterData>();
                var cretureGroup = MetaData<TableLabyrinthFormationMeta>.Data.GetStageInfo(metaId);
                if (cretureGroup == null)
                {
                    Logger.ErrorLog($"GetNodePlayData cretureGroup Is Null MetaId :{metaId}");
                    return playDataInfo;
                }

                for (int i = 0; i < cretureGroup.AppearCreatureIds.Count; ++i)
                {
                    var randLevel = RandomGenerator.GetRandomValue(cretureGroup.CreatureLv[0], cretureGroup.CreatureLv[1] + 1);
                    playDataInfo.nodeMonsterDatas.Add(new LabyrinthNodeModel.NodeMonsterData()
                    {
                        slotIdx = i,
                        creatureMetaId = cretureGroup.AppearCreatureIds[i],
                        level = randLevel,
                        hp = 10000
                    });
                }
            }

            return playDataInfo;
        }

        public class BattleResultInfo
        {
            public int addWeeklyPoint { get; set; }
            public List<Item> rewardItems { get; set; }
            public NodeState state { get; set; }
            public EResultType result { get; set; }
        }

        public class LabyrintRewardInfo
        {
            public EResultType result { get; set; }
            public List<Item> rewardList { get; set; }
            public int addWeeklyPoint { get; set; }
            public int addTotalBroochCount { get; set; }
        }

        public static LabyrintRewardInfo GetLabyrintRewardList(Labyrinth userlabyrinthInfo, MasterData.Common.LabyrinthMeta labyrinthMeta)
        {
            LabyrintRewardInfo rewardInfo = new LabyrintRewardInfo();
            rewardInfo.rewardList = new List<Item>();
            rewardInfo.addWeeklyPoint = 0;
            rewardInfo.addTotalBroochCount = 0;
            rewardInfo.result = EResultType.Success;
            var weeklyMaxPoint = MetaData<TableGameConfigMeta>.Data.GetIntConf(ConfigType.LabyrinthWeeklyMaxPointCount);
            if (weeklyMaxPoint > 0 && userlabyrinthInfo.weeklyPoint < weeklyMaxPoint && labyrinthMeta.RewardBroochCount > 0)
            {
                var broochMetaId = MetaData<TableGameConfigMeta>.Data.GetIntConf(ConfigType.Labyrinth_Brooch_MetaID);
                if (broochMetaId <= 0)
                {
                    Logger.ErrorLog($"GetLabyrintRewardList GetIntConf Is Null ConfigType :{ConfigType.Labyrinth_Brooch_MetaID}");
                    rewardInfo.result = EResultType.MetaDataError;
                    return rewardInfo;
                }
                var getItemInfo = MetaData<TableItemMeta>.Data.GetItem(broochMetaId);
                if (getItemInfo == null)
                {
                    Logger.ErrorLog($"GetLabyrintRewardList GetItem Is Null MetaId :{broochMetaId}");
                    rewardInfo.result = EResultType.GetLabyrintRewardListItemMetaFailed;
                    return rewardInfo;
                }

                rewardInfo.addWeeklyPoint = labyrinthMeta.GetWeeklyPoint;
                rewardInfo.addTotalBroochCount = labyrinthMeta.RewardBroochCount;
                rewardInfo.rewardList.Add(new Item()
                {
                    userIdx = userlabyrinthInfo.userIdx,
                    metaId = broochMetaId,
                    count = labyrinthMeta.RewardBroochCount,
                });

                userlabyrinthInfo.totalBroochCount += rewardInfo.addTotalBroochCount;
                userlabyrinthInfo.weeklyPoint += rewardInfo.addWeeklyPoint;
            }

            if (labyrinthMeta.RewardGroupId > 0)
            {
                var rewardGroupMetaDatas = MetaData<TableRewardMeta>.Data.GetRewardList(labyrinthMeta.RewardGroupId);
                if (rewardGroupMetaDatas == null || rewardGroupMetaDatas.Count <= 0)
                {
                    Logger.ErrorLog($"GetLabyrintRewardList rewardGroupMetaDatas Is Null rewardGroupId :{labyrinthMeta.RewardGroupId}");
                    rewardInfo.result = EResultType.GetLabyrintRewardListRewardMetaFailed;
                    return rewardInfo;
                }
                foreach (var rewardGroupMetaData in rewardGroupMetaDatas)
                {
                    var getItemInfo = MetaData<TableItemMeta>.Data.GetItem(rewardGroupMetaData.RewardItemId);
                    if (getItemInfo == null)
                    {
                        Logger.ErrorLog($"GetLabyrintRewardList GetItem Is Null MetaId :{rewardGroupMetaData.RewardItemId}");
                        rewardInfo.result = EResultType.GetLabyrintRewardListItemMetaFailed;
                        break;
                    }

                    rewardInfo.rewardList.Add(new Item()
                    {
                        userIdx = userlabyrinthInfo.userIdx,
                        metaId = rewardGroupMetaData.RewardItemId,
                        count = rewardGroupMetaData.RewardItemCount[0]
                    });
                }
            }

            return rewardInfo;
        }

        public static async Task<BattleResultInfo> BattleResult(Actor actor, BaseDatabaseConnection connection, Labyrinth userlabyrinthInfo, MasterData.Common.LabyrinthMeta labyrinthMeta, CS_LabyrinthBattleEnd req, DateTime now, bool isArtifact = false)
        {
            BattleResultInfo resultInfo = new BattleResultInfo();
            resultInfo.result = EResultType.Success;
            resultInfo.rewardItems = new List<Item>();
            resultInfo.state = NodeState.Idle;
            var getUserCharacter = actor.LabyrinthCharacterModel.GetLabyrinthCharacter(req.CharIdx);
            if (getUserCharacter == null)
            {
                Logger.ErrorLog($"BattleResult GetLabyrinthCharacter Null UserIdx:{actor.UserModel.UserIndex}");
                resultInfo.result = EResultType.LabyrinthGetLabyrinthCharacterNullFailed;
                return resultInfo;
            }

            int affectd_cnt = 0;
            resultInfo.addWeeklyPoint = 0;
            var addTotalBroochCount = 0;
            List<MonsterLog> monsterLogs = new List<MonsterLog>();

            var getUserNode = actor.LabyrinthNodeModel.GetLabyrinthNode(userlabyrinthInfo.nodeStage, userlabyrinthInfo.currentRow);
            if (req.CharHp > 0)
            {
                var rewardList = GetLabyrintRewardList(userlabyrinthInfo, labyrinthMeta);
                if (rewardList.result != EResultType.Success)
                {
                    Logger.ErrorLog($"BattleResult GetLabyrinthNode EResult Type : {rewardList.result}, UserIdx:{actor.UserModel.UserIndex}");
                    resultInfo.result = rewardList.result;
                    return resultInfo;
                }
                resultInfo.addWeeklyPoint = rewardList.addWeeklyPoint;
                resultInfo.rewardItems.AddRange(rewardList.rewardList);
                addTotalBroochCount = rewardList.addTotalBroochCount;
                if (labyrinthMeta.RewardArtifactGroup > 0)
                {
                    var getArtifactInfos = await GetNodePlayData(NodeType.Artifact, labyrinthMeta.RewardArtifactGroup);
                    if (getArtifactInfos != null)
                    {
                        getUserNode.monsterData = JsonConvert.SerializeObject(getArtifactInfos.nodeArtifactDatas);
                        getUserNode.state = NodeState.Reward;
                    }
                }
                else
                {
                    getUserNode.state = NodeState.Complete;
                }

                resultInfo.state = getUserNode.state;

                getUserCharacter.hpPercent = req.CharHp;
                getUserCharacter.updateTime = now;

                switch (labyrinthMeta.NodeType)
                {
                    //case NodeType.Normal:
                    //    await actor.MissionModel.UpdateCollection(connection, MasterData.Types.ClearType.LabyrinthNormalNodeClearCount, 1);
                    //    break;
                    //case NodeType.Elite:
                    //    await actor.MissionModel.UpdateCollection(connection, MasterData.Types.ClearType.LabyrinthEliteNodeClearCount, 1);
                    //    break;
                    case NodeType.Boss:
                        //var clearTypeValue = MetaData<TableMissionMeta>.Data.GetClearTypeValue(ClearType.LabyrinthSpecificFloorClearCount);
                        //if (userlabyrinthInfo.floor == clearTypeValue)
                        //{
                        //    await actor.MissionModel.UpdateCollection(connection, MasterData.Types.ClearType.LabyrinthSpecificFloorClearCount, 1);
                        //}

                        //await actor.MissionModel.UpdateCollection(connection, MasterData.Types.ClearType.LabyrinthBossNodeClearCount, 1);
                        await actor.MissionModel.UpdateCollection(connection, MasterData.Types.ClearType.LabyrinthFloorClearCount, 1);
                        break;
                }

                await actor.MissionModel.UpdateCollection(connection, MasterData.Types.ClearType.LabyrinthNodeClearCount, 1);
            }
            else
            {
                var labyrinthCharInfo = actor.LabyrinthCharacterModel.GetLabyrinthCharacter(req.CharIdx);
                if (labyrinthCharInfo == null)
                {
                    Logger.ErrorLog($"BattleResult labyrinthCharInfo Null UserIdx:{actor.UserModel.UserIndex}");
                    resultInfo.result = EResultType.LabyrinthGetLabyrinthCharacterNullFailed;
                    return resultInfo;
                }

                labyrinthCharInfo.hpPercent = req.CharHp <= 0 ? 0 : req.CharHp;
                List<LabyrinthNodeModel.NodeMonsterData> monsterData = JsonConvert.DeserializeObject<List<LabyrinthNodeModel.NodeMonsterData>>(getUserNode.monsterData);
                Dictionary<int, LabyrinthNodeModel.NodeMonsterData> dicmMonsterData = monsterData.ToDictionary(i => i.slotIdx);

                foreach (var data in dicmMonsterData.Values)
                {
                    if (data.creatureMetaId > 0)
                    {
                        data.hp = 0;
                    }
                }

                for (int i = 0; i < req.LabyrinthMonsterDatasLength; ++i)
                {
                    var reqMonsterData = req.LabyrinthMonsterDatas(i);
                    if (reqMonsterData.HasValue == true)
                    {
                        LabyrinthNodeModel.NodeMonsterData _nodeMonsterData = null;
                        dicmMonsterData.TryGetValue(reqMonsterData.Value.SlotIndex, out _nodeMonsterData);
                        if (_nodeMonsterData == null)
                        {
                            Logger.ErrorLog($"BattleResult _nodeMonsterData Null Error SlotIndex : {reqMonsterData.Value.SlotIndex}, UserIdx:{actor.UserModel.UserIndex}");
                            resultInfo.result = EResultType.RequestError;
                            break;
                        }
                        if (_nodeMonsterData.creatureMetaId > 0)
                        {
                            _nodeMonsterData.hp = reqMonsterData.Value.Hp < 0 ? 0 : reqMonsterData.Value.Hp;
                        }

                        monsterLogs.Add(new MonsterLog()
                        {
                            SlotIdx = _nodeMonsterData.slotIdx,
                            CreatureId = _nodeMonsterData.creatureMetaId,
                            Hp = _nodeMonsterData.hp,
                            Level = _nodeMonsterData.level,
                        });
                    }
                }

                if (resultInfo.result != EResultType.Success)
                {
                    return resultInfo;
                }

                var jsonString = JsonConvert.SerializeObject(dicmMonsterData.Values);

                getUserCharacter.hpPercent = 0;
                getUserCharacter.updateTime = now;
                getUserNode.monsterData = jsonString;
            }


            affectd_cnt = await DBGameLabyrinthNodeInfo.UpdateLabyrinthPlayData(connection, actor.UserModel.UserIndex, userlabyrinthInfo.nodeStage, userlabyrinthInfo.currentRow, getUserNode.state, getUserNode.monsterData);
            if (affectd_cnt <= 0)
            {
                Logger.ErrorLog($"BattleResult DB Update Error LabyrinthPlayData UserIdx:{actor.UserModel.UserIndex}");
                resultInfo.result = EResultType.BattleResultDBUpdateFailed;
                return resultInfo;
            }

            affectd_cnt = await DBGameLabyrinthCharacterInfo.UpdateCharacterHp(connection, getUserCharacter);
            if (affectd_cnt <= 0)
            {
                Logger.ErrorLog($"BattleResult DB Update Error CharacterHp UserIdx:{actor.UserModel.UserIndex}");
                resultInfo.result = EResultType.BattleResultDBUpdateFailed;
                return resultInfo;
            }

            affectd_cnt = await DBGameLabyrinthInfo.InsertOrUpdateLabyrinth(connection, userlabyrinthInfo);
            if (affectd_cnt <= 0)
            {
                Logger.ErrorLog($"BattleResult DB Update Error Labyrinth UserIdx:{actor.UserModel.UserIndex}");
                resultInfo.result = EResultType.BattleResultDBUpdateFailed;
                return resultInfo;
            }

            long refrenceLogId = Uniquekey.Get();

            if (resultInfo.rewardItems.Count > 0)
            {
                List<ItemDataLog> addItemLogs = new List<ItemDataLog>();
                affectd_cnt = await DBGameItemInfo.UpdateItems(connection, resultInfo.rewardItems);
                if (affectd_cnt <= 0)
                {
                    Logger.ErrorLog($"BattleResult DB Update Error Items UserIdx:{actor.UserModel.UserIndex}");
                    resultInfo.result = EResultType.BattleResultDBUpdateFailed;
                    return resultInfo;
                }

                foreach (var rewardItem in resultInfo.rewardItems)
                {
                    actor.ItemModel.SetItem(rewardItem);

                    addItemLogs.Add(new ItemDataLog()
                    {
                        MetaId = rewardItem.metaId,
                        AddCount = rewardItem.count,
                        ResultCount = actor.ItemModel.GetItem(rewardItem.metaId).count
                    });

                    await RankingMission.UpdateRankingItemMission(connection, actor.UserModel.UserIndex, RankingConditionType.GetItem, rewardItem.metaId, rewardItem.count, now);
                }

                await SqsModel.GetInstance().SendLog<ItemLog>(new ItemLog()
                {
                    UserIdx = actor.UserModel.UserIndex,
                    LogTime = now,
                    ActionType = ActionType.LabyrinthBattleSuccess,
                    LogType = LogType.Item,
                    ItemLogDatas = addItemLogs,
                    RefrenceLogId = refrenceLogId
                });
            }

            if (resultInfo.addWeeklyPoint > 0)
            {
                actor.LabyrinthModel.SetUserLabyrinth(userlabyrinthInfo);
            }


            actor.LabyrinthCharacterModel.SetLabyrinthCharacter(getUserCharacter);
            actor.LabyrinthNodeModel.SetNodeDate(getUserNode);

            if (req.CharHp > 0)
            {
                await SqsModel.GetInstance().SendLog<LabyrinthBattleSuccessLog>(new LabyrinthBattleSuccessLog()
                {
                    UserIdx = actor.UserModel.UserIndex,
                    LogTime = now,
                    ActionType = ActionType.LabyrinthBattleSuccess,
                    LogType = LogType.LabyrinthBattle,
                    RefrenceLogId = refrenceLogId,
                    Floor = userlabyrinthInfo.floor,
                    NodeStage = userlabyrinthInfo.nodeStage,
                    CurrentRow = userlabyrinthInfo.currentRow,
                    AddWeeklyPoint = resultInfo.addWeeklyPoint,
                    ResultWeeklyPoint = userlabyrinthInfo.weeklyPoint,
                    AddTotalBroochCount = addTotalBroochCount,
                    AddRankingPoint = labyrinthMeta.GetRankingPoint,
                    ResultTotalBroochCount = userlabyrinthInfo.totalBroochCount,
                    LabyrinthId = getUserNode.labyrinthMetaId,
                    LabyrinthFormationId = getUserNode.labyrinthFormationMetaId,
                    NodeType = (int)labyrinthMeta.NodeType,
                    CharSlotIdx = getUserCharacter.slotIndex,
                    CharIdx = getUserCharacter.charIdx,
                    CharHpPercent = getUserCharacter.hpPercent,
                });
                await RankingMission.UpdateRankingLabyrinthMission(connection, actor.UserModel.UserIndex, RankingConditionType.LabyrinthRankingPoint, labyrinthMeta.GetRankingPoint, now);
            }

            return resultInfo;
        }

        public class LabyrintEntryInfo
        {
            public bool isResestDaily { get; set; }
            public bool isCheckEntry { get; set; }
            public int entryCount { get; set; }
            public Item useItem { get; set; }
            public EResultType resultType { get; set; }
        }

        public static LabyrintEntryInfo UseEntryCount(Actor actor, DateTime now)
        {
            LabyrintEntryInfo entryInfo = new LabyrintEntryInfo();
            entryInfo.isCheckEntry = false;
            entryInfo.isResestDaily = false;
            entryInfo.resultType = EResultType.Success;
            var getUserLabyrinth = actor.LabyrinthModel.GetUserLabyrinth();
            if (getUserLabyrinth == null)
            {
                Logger.ErrorLog($"UseEntryCount getUserLabyrinth is Null UserIdx: {actor.UserModel.UserIndex}");
                entryInfo.resultType = EResultType.UseEntryCountUserLabyrinthFailed;
                return entryInfo;
            }

            entryInfo.entryCount = getUserLabyrinth.entryCount;
            if (getUserLabyrinth.dailyRefreshTime.Date < now.Date)
            {
                var getRefreshCount = MetaData<TableGameConfigMeta>.Data.GetIntConf(MasterData.Types.ConfigType.Labyrinth_Daily_Reset_Count);
                if (getRefreshCount <= 0)
                {
                    Logger.ErrorLog($"UseEntryCount getRefreshCount is Zero : {getRefreshCount}");
                    entryInfo.resultType = EResultType.UseEntryCountGameConfigFailed;
                    return entryInfo;
                }

                entryInfo.isResestDaily = true;
                entryInfo.entryCount = getRefreshCount;
            }

            if (entryInfo.entryCount <= 0)
            {
                var labyrinthKey = actor.ItemModel.GetItem(GameShareDefine.LabyrinthKeyItem);
                if (labyrinthKey == null || labyrinthKey.count <= 0)
                {
                    entryInfo.resultType = EResultType.UseEntryCountEntryFailed;
                    return entryInfo;
                }
                else
                {
                    entryInfo.isCheckEntry = true;
                    entryInfo.useItem = new Item()
                    {
                        userIdx = actor.UserModel.UserIndex,
                        metaId = GameShareDefine.LabyrinthKeyItem,
                        count = -1,
                    };
                }
            }
            else
            {
                entryInfo.entryCount -= 1;
                entryInfo.isCheckEntry = true;
            }
            return entryInfo;
        }
    }
}
