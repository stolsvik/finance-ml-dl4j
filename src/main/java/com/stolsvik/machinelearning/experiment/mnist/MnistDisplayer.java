package com.stolsvik.machinelearning.experiment.mnist;

import javax.swing.*;
import java.awt.*;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.geom.AffineTransform;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.awt.image.RenderedImage;

/**
 * Displays all the 60k training images and 10k test images from MNIST in two different windows.
 * 
 * @author Endre Stølsvik, http://endre.stolsvik.com, 2017-03-14 23:36
 */
public class MnistDisplayer extends JFrame {
    private MnistImages _mnistImages;

    public static void main(String... args) {
        SwingUtilities.invokeLater(() -> new MnistDisplayer(MnistImages.getTraining()));
        SwingUtilities.invokeLater(() -> new MnistDisplayer(MnistImages.getTest()));
    }

    public MnistDisplayer(MnistImages mnistImages) {
        _mnistImages = mnistImages;
        this.setTitle("MNIST [" + mnistImages.getType() + "]");
        this.setLayout(new FlowLayout());

        int numberOfImages = _mnistImages.getSize();

        int numCols = 65;
        int numRows = ((int) Math.ceil(numberOfImages / 65d));
        int sizeXY = 29;

        int width = numCols * sizeXY - 1;
        int height = numRows * sizeXY - 1;

        BufferedImage grayImage = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_GRAY);
        byte[] byteBuffer = ((DataBufferByte) grayImage.getRaster().getDataBuffer()).getData();
        for (int i = 1; i < numRows; i++) {
            int yStrided = (i * sizeXY - 1) * width;
            for (int j = 0; j < width; j++) {
                byteBuffer[yStrided + j] = 64;
            }
        }
        for (int i = 0; i < height; i++) {
            int yStrided = i * width;
            for (int j = 1; j < numCols; j++) {
                byteBuffer[yStrided + (j * sizeXY - 1)] = 64;
            }
        }
        for (int i = 0; i < numberOfImages; i++) {
            int imgX = i % numCols;
            int imgY = i / numCols;
            int startX = imgX * sizeXY;
            int startY = imgY * sizeXY;
            int startYStrided = startY * width;
            int startXYStrided = startYStrided + startX;
            byte[] img = _mnistImages.getImage(i);
            for (int y = 0; y < 28; y++) {
                for (int x = 0; x < 28; x++) {
                    byteBuffer[startXYStrided + (y * width) + x] = img[y * 28 + x];
                }
            }
        }

        RenderedImagePanel imagePane = new RenderedImagePanel(grayImage);
        JScrollPane scroller = new JScrollPane(imagePane,
                ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS, ScrollPaneConstants.HORIZONTAL_SCROLLBAR_NEVER);
        scroller.setPreferredSize(new Dimension(grayImage.getWidth() + 50, 1200));
        this.add(scroller);
        this.pack();
        this.setVisible(true);
        this.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
    }

    public class RenderedImagePanel extends JPanel implements Scrollable {

        private RenderedImage _image;

        public RenderedImagePanel(RenderedImage image) {
            _image = image;
            setAutoscrolls(true);
            MouseAdapter ma = new MouseAdapter() {

                private Point origin;

                @Override
                public void mousePressed(MouseEvent e) {
                    origin = new Point(e.getPoint());
                }

                @Override
                public void mouseReleased(MouseEvent e) {
                    /* no-op */
                }

                @Override
                public void mouseDragged(MouseEvent e) {
                    if (origin != null) {
                        JViewport viewPort = (JViewport) SwingUtilities.getAncestorOfClass(JViewport.class,
                                RenderedImagePanel.this);
                        if (viewPort != null) {
                            Rectangle view = viewPort.getViewRect();
                            int posY = e.getY() - view.y;
                            if ((posY > view.height) || (posY < 0)) {
                                int moveY = posY > 0 ? -15 : +15;
                                Rectangle r = new Rectangle(e.getX(), view.y + (posY < 0 ? view.height : 0) + moveY,
                                        1, 1);
                                origin = new Point(origin.x, origin.y + moveY);
                                scrollRectToVisible(r);
                                return;
                            }

                            int deltaX = origin.x - e.getX();
                            int deltaY = origin.y - e.getY();

                            view.x += deltaX;
                            view.y += deltaY;

                            scrollRectToVisible(view);
                        }
                    }
                }

            };

            addMouseListener(ma);
            addMouseMotionListener(ma);
        }

        @Override
        public Dimension getPreferredSize() {
            return new Dimension(_image.getWidth(), _image.getHeight());
        }

        @Override
        protected void paintComponent(Graphics g) {
            super.paintComponent(g);
            Graphics2D g2d = (Graphics2D) g.create();
            g2d.drawRenderedImage(_image, new AffineTransform());
            g2d.dispose();
        }

        @Override
        public Dimension getPreferredScrollableViewportSize() {
            return getPreferredSize();
        }

        @Override
        public int getScrollableUnitIncrement(Rectangle visibleRect, int orientation, int direction) {
            return 29;
        }

        @Override
        public int getScrollableBlockIncrement(Rectangle visibleRect, int orientation, int direction) {
            return 29 * 25;
        }

        @Override
        public boolean getScrollableTracksViewportWidth() {
            return false;
        }

        @Override
        public boolean getScrollableTracksViewportHeight() {
            return false;
        }
    }
}
