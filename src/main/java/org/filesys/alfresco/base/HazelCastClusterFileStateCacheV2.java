/*
 * Copyright (C) 2006-2011 Alfresco Software Limited.
 *
 * This file is part of Alfresco
 *
 * Alfresco is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Alfresco is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with Alfresco. If not, see <http://www.gnu.org/licenses/>.
 */

package org.filesys.alfresco.base;

import java.util.concurrent.*;

import org.filesys.server.filesys.FileAccessToken;
import org.filesys.server.filesys.FileStatus;
import org.filesys.server.filesys.cache.cluster.*;
import org.filesys.server.filesys.cache.hazelcast.*;
import org.filesys.smb.OpLockType;

import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.core.MessageListener;

/**
 * HazelCast V2 Clustered File State Cache Class
 *
 * @author gkspencer
 */
public class HazelCastClusterFileStateCacheV2 extends HazelCastClusterFileStateCache implements MembershipListener,
					EntryListener<String, HazelCastClusterFileState>, MessageListener<ClusterMessage> {

	/**
	 * Class constructor
	 * 
	 */
	public HazelCastClusterFileStateCacheV2() {
		
	}
	
	/**
	 * Start the cluster
	 *
	 * @throws Exception
	 */
	public void startCluster()
			throws Exception {

		super.startCluster();

		// Signal that the cluster cache is running, this will mark the filesystem as available
		if (m_stateCache != null && m_clusterTopic != null) {

			// Add a listener to receive cluster cache entry events
			m_stateCache.addEntryListener(this, false);

			// Add a listener to receive cluster messages via the topic
			m_clusterTopic.addMessageListener(this);

			// Indicate that the cluster is running
			getStateCache().clusterRunning();
		}
	}

	/**
	 * Execute a rename file state
	 *
	 * @param oldPath String
	 * @param newPath String
	 * @param isDir boolean
	 * @return boolean
	 * @exception InterruptedException
	 * @exception ExecutionException
	 */
	public boolean executeRenameFileState( String oldPath, String newPath, boolean isDir)
			throws InterruptedException, ExecutionException {

		// Rename the state via a remote call to the node that owns the file state
		ExecutorService execService = m_hazelCastInstance.getExecutorService();
		Callable<Boolean> callable = new RenameStateTask( getClusterName(), oldPath, newPath, isDir, hasTaskDebug(), hasTaskTiming());
		FutureTask<Boolean> renameStateTask = new DistributedTask<Boolean>( callable, oldPath);

		execService.execute( renameStateTask);

		return renameStateTask.get().booleanValue();
	}

	/**
	 * Execute adding an oplock
	 *
	 * @param path String
	 * @param remoteOpLock RemoteOpLockDetails
	 * @return boolean
	 * @exception InterruptedException
	 * @exception ExecutionException
	 */
	public boolean executeAddOpLock( String path, RemoteOpLockDetails remoteOpLock)
			throws InterruptedException, ExecutionException {

		// Add the oplock via a remote call to the node that owns the file state
		ExecutorService execService = m_hazelCastInstance.getExecutorService();
		Callable<Boolean> callable = new AddOpLockTask( getClusterName(), path, remoteOpLock, hasTaskDebug(), hasTaskTiming());
		FutureTask<Boolean> addOpLockTask = new DistributedTask<Boolean>( callable,	path);

		execService.execute( addOpLockTask);

		return addOpLockTask.get().booleanValue();
	}

	/**
	 * Execute clear oplock
	 *
	 * @param path String
	 * @exception InterruptedException
	 * @exception ExecutionException
	 */
	public void executeClearOpLock( String path)
			throws InterruptedException, ExecutionException {

        // Remove the oplock using a remote call to the node that owns the file state
        ExecutorService execService = m_hazelCastInstance.getExecutorService();
        Callable<Boolean> callable = new RemoveOpLockTask( getClusterName(), path, hasTaskDebug(), hasTaskTiming());
        FutureTask<Boolean> removeOpLockTask = new DistributedTask<Boolean>( callable, path);

        execService.execute( removeOpLockTask);

        removeOpLockTask.get();
	}

	/**
	 * Execute an add lock
	 *
	 * @param path String
	 * @param lock ClusterFileLock
	 * @return ClusterFileState
	 * @exception InterruptedException
	 * @exception ExecutionException
	 */
	public ClusterFileState executeAddLock(String path, ClusterFileLock lock)
			throws InterruptedException, ExecutionException {

        // Add the oplock via a remote call to the node that owns the file state
        ExecutorService execService = m_hazelCastInstance.getExecutorService();
        Callable<ClusterFileState> callable = new AddFileByteLockTask( getClusterName(), path, lock,
                hasDebugLevel( DebugByteLock), hasTaskTiming());
        FutureTask<ClusterFileState> addLockTask = new DistributedTask<ClusterFileState>( callable, path);

        execService.execute( addLockTask);

        return addLockTask.get();
	}

	/**
	 * Execute a remove lock
	 *
	 * @param path String
	 * @param lock ClusterFileLock
	 * @return ClusterFileState
	 * @exception InterruptedException
	 * @exception ExecutionException
	 */
	public ClusterFileState executeRemoveLock( String path, ClusterFileLock lock)
			throws InterruptedException, ExecutionException {

        // Remove the lock via a remote call to the node that owns the file state
        ExecutorService execService = m_hazelCastInstance.getExecutorService();
        Callable<ClusterFileState> callable = new RemoveFileByteLockTask( getClusterName(), path, lock,
                hasDebugLevel( DebugByteLock), hasTaskTiming());
        FutureTask<ClusterFileState> removeLockTask = new DistributedTask<ClusterFileState>( callable, path);

        execService.execute( removeLockTask);

        return removeLockTask.get();
	}

	/**
	 * Execute an oplock change type
	 *
	 * @param path String
	 * @param newTyp OpLockType
	 * @return OpLockType
	 * @exception InterruptedException
	 * @exception ExecutionException
	 */
	public OpLockType executeChangeOpLockType(String path, OpLockType newTyp)
			throws InterruptedException, ExecutionException {

        // Run the file access checks via the node that owns the file state
        ExecutorService execService = m_hazelCastInstance.getExecutorService();
        Callable<Integer> callable = new ChangeOpLockTypeTask( getClusterName(), path, newTyp, hasTaskDebug(), hasTaskTiming());
        FutureTask<Integer> changeOpLockTask = new DistributedTask<Integer>( callable, path);

        execService.execute( changeOpLockTask);

        return OpLockType.fromInt( changeOpLockTask.get().intValue());
	}

	/**
	 * Execute a grant file access
	 *
	 * @param path String
	 * @param params GrantAccessParams
	 * @return HazelCastAccessToken
	 * @exception InterruptedException
	 * @exception ExecutionException
	 */
	public HazelCastAccessToken executeGrantFileAccess( String path, GrantAccessParams params)
			throws InterruptedException, ExecutionException {

        // Run the file access checks via the node that owns the file state
        ExecutorService execService = m_hazelCastInstance.getExecutorService();
        Callable<FileAccessToken> callable = new GrantFileAccessTask( getClusterName(), path, params, hasTaskDebug(), hasTaskTiming());
        FutureTask<FileAccessToken> grantAccessTask = new DistributedTask<FileAccessToken>( callable, path);

        execService.execute( grantAccessTask);

        return (HazelCastAccessToken) grantAccessTask.get();
	}

	/**
	 * Execute release file access
	 *
	 * @param path String
	 * @param token HazelCastAccessToken
	 * @return int
	 * @exception InterruptedException
	 * @exception ExecutionException
	 */
	public int executeReleaseFileAccess( String path, HazelCastAccessToken token)
			throws InterruptedException, ExecutionException {

        // Run the file access checks via the node that owns the file state
        ExecutorService execService = m_hazelCastInstance.getExecutorService();
        Callable<Integer> callable = new ReleaseFileAccessTask( getClusterName(), path, token, m_topicName,
                hasDebugLevel( DebugFileAccess), hasTaskTiming());
        FutureTask<Integer> releaseAccessTask = new DistributedTask<Integer>( callable, path);

        execService.execute( releaseAccessTask);

        return releaseAccessTask.get().intValue();
	}

	/**
	 * Execute check file access
	 *
	 * @param path String
	 * @param chkLock ClusterFileLock
	 * @param writeChk boolean
	 * @return boolean
	 * @exception InterruptedException
	 * @exception ExecutionException
	 */
	public boolean executeCheckFileAccess( String path, ClusterFileLock chkLock, boolean writeChk)
			throws InterruptedException, ExecutionException {

        // Check the file access via a remote call to the node that owns the file state
        ExecutorService execService = m_hazelCastInstance.getExecutorService();
        Callable<Boolean> callable = new CheckFileByteLockTask( getClusterName(), path, chkLock, writeChk,
                hasDebugLevel( DebugFileAccess), hasTaskTiming());
        FutureTask<Boolean> checkLockTask = new DistributedTask<Boolean>( callable, path);

        execService.execute( checkLockTask);

        return checkLockTask.get().booleanValue();
	}

	/**
	 * Exceute a remote update state
	 *
	 * @param path String
	 * @param fileSts FileStatus
	 * @return boolean
	 */
	public boolean executeRemoteUpdateState( String path, FileStatus fileSts)
			throws InterruptedException, ExecutionException {

        // Update the file status via a remote call to the node that owns the file state
        ExecutorService execService = m_hazelCastInstance.getExecutorService();
        Callable<Boolean> callable = new UpdateStateTask( getClusterName(), path, fileSts,
                hasDebugLevel( DebugRemoteTask | DebugFileStatus), hasTaskTiming());
        FutureTask<Boolean> updateStateTask = new DistributedTask<Boolean>( callable, path);

        execService.execute( updateStateTask);

        return updateStateTask.get().booleanValue();
	}

	/**
	 * Execute update file data status
	 *
	 * @param path String
	 * @param startUpdate boolean
	 * @return boolean
	 */
	public boolean executeUpdateFileDataStatus( String path, boolean startUpdate)
			throws InterruptedException, ExecutionException {

        // Set the file data update status via a remote call to the node that owns the file state
        ExecutorService execService = m_hazelCastInstance.getExecutorService();
        Callable<Boolean> callable = new FileDataUpdateTask( getClusterName(), path, getLocalNode(), startUpdate,
                hasDebugLevel( DebugFileDataUpdate), hasTaskTiming());
        FutureTask<Boolean> fileDataUpdateTask = new DistributedTask<Boolean>( callable, path);

        execService.execute( fileDataUpdateTask);

        return fileDataUpdateTask.get().booleanValue();
	}
}
