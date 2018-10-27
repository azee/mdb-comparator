package ru.greatbit.mdb.comparator.ui;

import ru.greatbit.mdb.comparator.Error;

import javax.swing.*;
import javax.swing.filechooser.FileNameExtensionFilter;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import static ru.greatbit.mdb.comparator.Comparer.compare;

public class Application extends JFrame {

    private Application() throws HeadlessException {
        createAndShowGUI();
    }

    private void createAndShowGUI() {
        JPanel panel = new JPanel();
        getContentPane().add(panel);

        panel.setLayout(null);

        JTextField file1 = new JTextField();
        file1.setBounds(20, 50, 190, 30);
        panel.add(file1);

        JTextField file2 = new JTextField();
        file2.setBounds(500, 50, 190, 30);
        panel.add(file2);

        JFileChooser fileSelector1 = new JFileChooser();
        fileSelector1.setFileFilter(
                new FileNameExtensionFilter("MS Access files", "mdb")
        );
        fileSelector1.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent event) {
                if (event.getActionCommand().equals(JFileChooser.APPROVE_SELECTION)){
                    file1.setText(fileSelector1.getSelectedFile().getAbsolutePath());
                }
            }
        });
        panel.add(fileSelector1);

        JFileChooser fileSelector2 = new JFileChooser();
        fileSelector2.setFileFilter(
                new FileNameExtensionFilter("MS Access files", "mdb")
        );
        fileSelector2.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent event) {
                if (event.getActionCommand().equals(JFileChooser.APPROVE_SELECTION)){
                    file2.setText(fileSelector2.getSelectedFile().getAbsolutePath());
                }
            }
        });
        panel.add(fileSelector2);

        JButton browseFile1 = new JButton("Browse");
        browseFile1.setBounds(20, 90, 100, 30);
        browseFile1.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent event) {
                fileSelector1.showOpenDialog(null);
            }
        });
        panel.add(browseFile1);

        JButton browseFile2 = new JButton("Browse");
        browseFile2.setBounds(500, 90, 100, 30);
        browseFile2.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent event) {
                fileSelector2.showOpenDialog(null);
            }
        });
        panel.add(browseFile2);

        JTextArea textArea = new JTextArea(10, 40);
        textArea.setBounds(20, 140, 700, 400);
        JScrollPane scroll = new JScrollPane(textArea);
        scroll.setBounds(20, 140, 700, 400);
        panel.add(scroll);

        JButton compareButton = new JButton("Compare");
        compareButton.setBounds(300, 90, 100, 30);
        compareButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent event) {
                try {
                    List<Error> errors = compare(file1.getText(), file2.getText());
                    textArea.setText(getErrorText(errors));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        panel.add(compareButton);

        setTitle("Compare MDB files");
        setSize(800, 600);
        setLocationRelativeTo(null);
        setDefaultCloseOperation(EXIT_ON_CLOSE);
    }

    private String getErrorText(List<Error> errors) {
        if (errors.size() == 0){
            return "Files are identical";
        } else {
            StringBuffer sb = new StringBuffer();
            sb.append("Found ").append(errors.size()).append(" errors\n");
            errors.forEach(error -> {
                sb.append(error.getMessage()).append("\n\n");
            });

            return sb.toString();
        }
    }

    public static void main(String[] args) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                Application application = new Application();
                application.setVisible(true);
            }
        }).start();
    }
}
