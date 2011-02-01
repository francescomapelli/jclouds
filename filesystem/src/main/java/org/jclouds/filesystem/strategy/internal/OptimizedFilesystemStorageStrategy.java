/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.jclouds.filesystem.strategy.internal;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.sun.org.apache.bcel.internal.classfile.SourceFile;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.filesystem.predicates.validators.FilesystemBlobKeyValidator;
import org.jclouds.filesystem.predicates.validators.FilesystemContainerNameValidator;
import org.jclouds.filesystem.reference.FilesystemConstants;
import org.jclouds.io.Payload;

/**
 *
 */
public class OptimizedFilesystemStorageStrategy  extends FilesystemStorageStrategyImpl {


    @Inject
    protected OptimizedFilesystemStorageStrategy(
        Blob.Factory blobFactory,
        @Named(FilesystemConstants.PROPERTY_BASEDIR) String baseDir,
        FilesystemContainerNameValidator filesystemContainerNameValidator,
        FilesystemBlobKeyValidator filesystemBlobKeyValidator) {

        super(blobFactory, baseDir, filesystemContainerNameValidator, filesystemBlobKeyValidator);
    }


    /**
     * Write a {@link Blob} {@link Payload} into a file
     * @param container
     * @param blobKey
     * @param payload
     * @throws IOException
     */
    @Override
    public void writePayloadOnFile(String container, String blobKey, Payload payload) throws IOException {
        filesystemContainerNameValidator.validate(container);
        filesystemBlobKeyValidator.validate(blobKey);

        
        File sourceFile = null;
        File destFile = null;

        sourceFile = (File) payload.getRawContent();

        destFile = getFileForBlobKey(container, blobKey);
        File parentDirectory = destFile.getParentFile();
        if (!parentDirectory.exists()) {
            if (!parentDirectory.mkdirs()) {
                throw new IOException("An error occurred creating directory [" + parentDirectory.getName() + "].");
            }
        }

        sourceFile.renameTo(destFile);

    }

}
